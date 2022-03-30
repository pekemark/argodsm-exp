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

	size_t undo_log::initialize(size_t offset) {
		durable_mem = reinterpret_cast<unsigned char*>(argo::virtual_memory::allocate_mappable(entry_size, entry_size));
		argo::virtual_memory::map_memory(durable_mem, entry_size, offset, PROT_READ|PROT_WRITE, argo::virtual_memory::memory_type::nvm);
		return entry_size;
	}

	void undo_log::record_changes(location_t location, char *modified_data, char *original_data) {
		memcpy(durable_mem, original_data, entry_size);
	}

}