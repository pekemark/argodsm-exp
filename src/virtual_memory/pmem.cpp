/**
 * @file
 * @brief This file implements the virtual memory and virtual address handling
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <system_error>
#include <unistd.h>

#include "../env/env.hpp"
#include "virtual_memory.hpp"

namespace {
	/* file constants */
	/** @todo hardcoded start address */
	const char* ARGO_START = (char*) 0x200000000000l;
	/** @todo hardcoded end address */
	const char* ARGO_END   = (char*) 0x600000000000l;
	/** @todo hardcoded size */
	const ptrdiff_t ARGO_SIZE = ARGO_END - ARGO_START;
	/** @todo hardcoded maximum size */
	const ptrdiff_t ARGO_SIZE_LIMIT = 0x80000000000l;

	/** @brief error message string */
	const std::string msg_alloc_fail    = "ArgoDSM could not allocate mappable memory";
	/** @brief error message string */
	const std::string msg_mmap_fail     = "ArgoDSM failed to map in virtual address space.";
	/** @brief error message string */
	const std::string msg_main_vm_fail  = "ArgoDSM failed to set up virtual memory. Please report a bug.";
	/** @brief error message string */
	const std::string msg_main_shm_fail = "ArgoDSM failed to set up shared memory. Please report a bug.";
	/** @brief error message string */
	const std::string msg_main_nvm_fail = "ArgoDSM failed to set up persistent memory. Please report a bug.";

	/* shm variables */
	/** @brief a file descriptor for backing the virtual address space used by ArgoDSM */
	int fd_shm;
	/** @brief the size of the ArgoDSM shared memory space     */
	std::size_t avail_shm;

	/* nvm variables */
	/** @brief a file descriptor for backing the virtual address space used by ArgoDSM */
	int fd_nvm;
	/** @brief the size of the ArgoDSM persistent memory space */
	std::size_t avail_nvm;

	/* vm variables  */
	/** @brief the address at which the virtual address space used by ArgoDSM starts   */
	void* start_addr;
	/** @brief the size of the ArgoDSM virtual address space   */
	std::size_t avail_vm = ARGO_SIZE_LIMIT;
}

#define ERR(msg) \
do { \
	std::cerr << msg << std::endl; \
	throw std::system_error(std::make_error_code(static_cast<std::errc>(errno)), msg); \
	exit(EXIT_FAILURE); \
} while (0)

namespace argo {
	namespace virtual_memory {
		void init_shm() {
			std::string filename = "/argocache" + std::to_string(getpid());
			fd_shm = shm_open(filename.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
			if(shm_unlink(filename.c_str())) {
				ERR(msg_main_shm_fail);
			}
			/* find maximum filesize */
			struct statvfs b;
			statvfs("/dev/shm", &b);
			avail_shm = b.f_bavail * b.f_bsize;
			if(ftruncate(fd_shm, avail_shm)) {
				ERR(msg_main_shm_fail);
			}
		}

		void init_nvm() {
			/* check if nvm path is given */
			std::string nvm_path = env::nvm_path();
			if(nvm_path.empty()) {
				throw std::invalid_argument(
					"Invalid nvm path");
			}
			
			/* try to open the given path */
			int flags = O_RDWR;
			fd_nvm = open(nvm_path.c_str(), flags, S_IRUSR|S_IWUSR);
			if (fd_nvm == -1) {
				/** @note fsdax: the nvm directory path should be given  */
				if (errno == ENOENT) {
					ERR(msg_main_nvm_fail);
				}
				/** @note fsdax: the mounted nvm directory path is given */
				if (errno == EISDIR) {
					flags |= O_CREAT|O_DIRECT|O_SYNC;
					std::string filename = nvm_path + "/argocache" + std::to_string(getpid());
					fd_nvm = open(filename.c_str(), flags, S_IRUSR|S_IWUSR);
					if (fd_nvm == -1) {
						ERR(msg_main_nvm_fail);
					}
					/** @note unlink only if don't want to recover the data */
					if(unlink(filename.c_str())) {
						ERR(msg_main_nvm_fail);
					}
					/* find maximum filesize */
					struct statvfs b;
					statvfs(nvm_path.c_str(), &b);
					avail_nvm = b.f_bavail * b.f_bsize;
					if(ftruncate(fd_nvm, avail_nvm)) {
						ERR(msg_main_nvm_fail);
					}
				} else {
					ERR(msg_main_nvm_fail);
				}
			}

			/* find the opened file type */
			struct stat stbuf;
			fstat(fd_nvm, &stbuf);
			if (!(S_ISREG(stbuf.st_mode) ||
			      S_ISCHR(stbuf.st_mode))) {
				/* neither regular file nor character device */
				ERR(msg_main_nvm_fail);
			}
		}

		void init_vm() {
			/** @todo check desired range is free */
			constexpr int flags = MAP_ANONYMOUS|MAP_SHARED|MAP_FIXED|MAP_NORESERVE;
			start_addr = ::mmap((void*)ARGO_START, avail_vm, PROT_NONE, flags, -1, 0);
			if(start_addr == MAP_FAILED) {
				ERR(msg_main_vm_fail);
			}
		}

		void init() {
			init_shm();
			init_nvm();
			init_vm();
		}

		void* start_address() {
			return start_addr;
		}

		std::size_t size() {
			return avail_vm;
		}

		int file_descriptor() {
			return fd_nvm;
		}

		void* allocate_mappable(std::size_t alignment, std::size_t size) {
			void* p;
			auto r = posix_memalign(&p, alignment, size);
			if(r || p == nullptr) {
				ERR(msg_alloc_fail);
			}
			return p;
		}

		void map_memory(void* addr, std::size_t size, std::size_t offset, int prot, int smem) {
			auto p = (smem == memory_type::shm)
				? ::mmap(addr, size, prot, MAP_SHARED|MAP_FIXED                  , fd_shm, offset)
				: ::mmap(addr, size, prot, MAP_SHARED_VALIDATE|MAP_SYNC|MAP_FIXED, fd_nvm, offset);
			if(p == MAP_FAILED) {
				ERR(msg_mmap_fail);
			}
		}
	} // namespace virtual_memory
} // namespace argo
