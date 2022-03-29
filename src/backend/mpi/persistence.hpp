#ifndef argo_persistence_hpp
#define argo_persistence_hpp argo_persistence_hpp

#include "types/types.hpp"

namespace argo::backend::persistence {

	template <
		typename location_t = argo::memory_t,
		size_t entry_size = 4096,
		size_t dirty_unit = 1
	>
	class undo_log {
	public:
		size_t initialize(size_t offset, size_t alignment) {return 0;}

		void record_changes(location_t location, char *modified_data, char *original_data = nullptr) {}
	};

}

#endif /* argo_persistence_hpp */