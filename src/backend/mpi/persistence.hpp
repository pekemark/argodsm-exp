#ifndef argo_persistence_hpp
#define argo_persistence_hpp argo_persistence_hpp

#include "types/types.hpp"

namespace argo::backend::persistence {

	template<size_t entry_size>
	struct durable_original;	

	class undo_log {

		using location_t = argo::memory_t;

		static const size_t entry_size = 4096; // TODO: From swdsm.cpp, should be imported from a header
		static const size_t dirty_unit = 1; // TODO: Should be imported from a header

		static const size_t entries = 256; // TODO: Should be imported from elsewhere or be part of initialisation

		durable_original<entry_size> *durable_mem;
	public:
		size_t initialize(size_t offset);

		void record_changes(location_t location, char *modified_data, char *original_data = nullptr);
	};

}

#endif /* argo_persistence_hpp */