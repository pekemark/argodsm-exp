#ifndef argo_persistence_hpp
#define argo_persistence_hpp argo_persistence_hpp

#include <unordered_map>

#include "../../synchronization/intranode/ticket_lock.hpp"
#include "types/types.hpp"

namespace argo::backend::persistence {

	template<size_t entry_size>
	struct durable_original;

	template<size_t entry_size, size_t dirty_unit>
	struct durable_change;

	struct durable_range;

	class range;

	template<typename location_t>
	struct group;

	class undo_log {

		using location_t = argo::memory_t;

		static const size_t entry_size = 4096; // TODO: From swdsm.cpp, should be imported from a header
		static const size_t dirty_unit = 1; // TODO: Should be imported from a header

		static const size_t alignment = entry_size; // TODO: Should be imported from a header

		static const size_t entries = 256; // TODO: Should be imported from elsewhere or be part of initialisation
		static const size_t groups = 64; // TODO: Should be imported from elsewhere or be part of initialisation
		static const size_t max_group_size = entries/groups;

		durable_original<entry_size> *d_original;
		durable_change<entry_size, dirty_unit> *d_change;
		location_t *d_location;

		range *entry_range;

		durable_range *d_group;
		range *group_range;
		group<location_t> *current_group;

		/** @brief Handling exclusive access for the structure. */
		locallock::ticket_lock *log_lock;

		template<typename T>
		static size_t durable_alloc(T *&addr, size_t copies, size_t offset);

	public:

		size_t initialize(size_t offset);

		void record_original(location_t location, char *original_data);
		void record_changes(location_t location, char *modified_data, char *original_data = nullptr);

	};

}

#endif /* argo_persistence_hpp */