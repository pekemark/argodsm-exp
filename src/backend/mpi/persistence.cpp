#include <atomic>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <unordered_map>

#include "../../synchronization/intranode/ticket_lock.hpp"
#include "../backend.hpp"
#include "virtual_memory/virtual_memory.hpp"

#include "persistence.hpp"

namespace argo::backend::persistence {

	/** @brief Container of original data to use for recovery.
	 * This data is generally only ever writen during normal operation
	 * but may be used to find modifications. Usually another reference
	 * is preferred, however.
	 * @tparam entry_size Size (in bytes) of data entries.
	*/
	template<size_t entry_size>
	struct durable_original {

		/** @brief Container of original, unmodified data. */
		char data[entry_size];

		/** @brief Copy data into the original container.
		 * @param source Pointer to the original data to copy from. Must be @c entry_size in length.
		*/
		void copy_data(char *source) {
			memcpy(this->data, source, entry_size);
		}

	};

	size_t undo_log::initialize(size_t offset) {
		static_assert(sizeof(durable_original<entry_size>) == entry_size,
			"The durable_original size doesn't match the entry_size.");
		durable_mem = reinterpret_cast<durable_original<entry_size>*>(
			argo::virtual_memory::allocate_mappable(entry_size, entries*sizeof(durable_original<entry_size>)));
		argo::virtual_memory::map_memory(durable_mem, entries*sizeof(durable_original<entry_size>),
			offset, PROT_READ|PROT_WRITE, argo::virtual_memory::memory_type::nvm);
		return entries*sizeof(durable_original<entry_size>);
	}

	void undo_log::record_changes(location_t location, char *modified_data, char *original_data) {
		durable_mem[(reinterpret_cast<uintptr_t>(location)/entry_size)%entries].copy_data(original_data);
	}

}