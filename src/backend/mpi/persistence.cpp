//#include <atomic>
#include <bitset>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <sys/mman.h>
#include <unordered_map>

#include "../../synchronization/intranode/ticket_lock.hpp"
#include "../backend.hpp"
#include "virtual_memory/virtual_memory.hpp"

#include "persistence.hpp"

namespace argo::backend::persistence {

	template<typename T>
	constexpr T div_ceil(T n, T d) { return (n-1)/d + 1; }

	template<typename T>
	constexpr T align_ceil(T n, T a) { return div_ceil(n, a) * a; }

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

	/** @brief Container of dirty bits for the correspoinding original data.
	 * A bit being one (1) means that the corresponding dirty unit
	 * in the original data needs to be reverted on recovery.
	 * This data is only ever update during normal operation
	 * (i.e., individual 0's becomes 1's or the entire map is reset).
	 * @tparam entry_size Size (in bytes) of data entries.
	 * @tparam dirty_unit Size (in bytes) each dirty bit cover.
	 * @note @p dirty_unit must divide @p entry_size.
	 */
	template<size_t entry_size, size_t dirty_unit>
	struct durable_change {

		static_assert((entry_size/dirty_unit)*dirty_unit == entry_size,
			"The dirty_unit doesn't divide the entry_size.");

		/** @brief Container of dirty bits. */
		std::bitset<entry_size/dirty_unit> map;

		static_assert(sizeof(map) == entry_size/dirty_unit/8,
			"The change map is under-utilised.");

		/** @brief Set dirty bits to 0. */
		void reset() {
			map.reset();
		}

		/** @brief Compare @p modified and @p original and set corresponding dirty bits.
		 * @param modified_data Pointer to modified data (e.g. in the ArgoDSM cache).
		 * @param original_data Pointer to original data (e.g. in the ArgoDSM write buffer).
		 * @note Avoid using data in the persistent log for the original. This is mainly for performance but may also affect correctness.
		 */
		void update(char *modified_data, char *original_data) {
			for (size_t map_index = 0; map_index < map.size(); ++map_index) {
				const size_t data_start = map_index * dirty_unit;
				for (size_t dirty_index = 0; dirty_index < dirty_unit ; ++dirty_index) {
					const size_t data_index = data_start + dirty_index;
					if (original_data[data_index] != modified_data[data_index]) {
						map.set(map_index); // The dirty unit has changed.
						break;
					}
				}
			}
		}

	};

	/** @brief Bookeeping structure to keep track of the state of a circular buffer.
	 * The internal representation stores enough information to determine
	 * the endpoints of the buffer and, in extereme cases, whether it is full or empty.
	 * Other redundant information (such as the maximum size)
	 * is not stored to minimise memory footprint.
	 * @note Current implementation allows endpoint indices to be
	 * betwen [0, 2*size] to differentiate between an empry and full buffer.
	 */
	struct durable_range {
		/** @brief Start index of the range (inclusive) plus multiple of size. */
		size_t start;
		/** @brief End index of the group (exclusive) plus multiple of size. */
		size_t end;

		durable_range(size_t start = 0)
		: start(start), end(start) {} // When empty, start and end are the same.

		size_t inline get_start(size_t size) { return start % size; }
		size_t inline get_end(size_t size) { return end % size; }

		bool inline is_empty() { return start == end; }
		bool inline is_full(size_t size) { return !is_empty() && get_start(size) == get_end(size); }

		size_t get_use(size_t size) {
			if (is_empty()) return 0;
			return ((get_end(size) + size) - get_start(size)) % size;
		}

	};

	class range {

		durable_range idx;
		size_t size;

		durable_range *durable;

	public:

		range(size_t size, size_t start = 0, durable_range *durable = nullptr)
		: idx(start), size(size), durable(durable) {
			if (start >= size)
				throw std::domain_error("The start index is greater than the size.");
			if (durable != nullptr) {
				durable->start = idx.start;
				durable->end = idx.end;
			}
		}

		size_t inline get_start() { return idx.get_start(size); }
		size_t inline get_end() { return idx.get_end(size); }

		bool inline is_empty() { return idx.is_empty(); }
		bool inline is_full() { return idx.is_full(size); }

		size_t get_use() { return idx.get_use(size); }

		void inc_start(size_t steps = 1) {
			// TODO: protect against bad steps
			idx.start = (idx.start + steps) % (2*size);
			if (durable != nullptr)
				durable->start = idx.start;
		}

		void inc_end(size_t steps = 1) {
			// TODO: protect against bad steps
			idx.end = (idx.end + steps) % (2*size);
			if (durable != nullptr)
				durable->end = idx.end;
		}

	};

	template<typename location_t>
	struct group {
		range entry_range;
		group(size_t entry_buffer_size, size_t entry_buffer_start, durable_range *d_group)
		: entry_range(entry_buffer_size, entry_buffer_start, d_group) {}
	};

	template<typename T>
	size_t undo_log::durable_alloc(T *&addr, size_t copies, size_t offset) {
		size_t size = align_ceil(copies*sizeof(T), alignment);
		addr = reinterpret_cast<T*>(argo::virtual_memory::allocate_mappable(entry_size, size));
		argo::virtual_memory::map_memory(addr, size, offset, PROT_READ|PROT_WRITE, argo::virtual_memory::memory_type::nvm);
		memset(addr, 0, size);
		return size;
	}

	size_t undo_log::initialize(size_t offset) {
		static_assert(sizeof(durable_original<entry_size>) == entry_size,
			"The durable_original size doesn't match the entry_size.");
		size_t init_offset = offset;
		init_offset += durable_alloc(d_original, entries, init_offset);
		// printf("d_original address: %p\n", d_original);
		init_offset += durable_alloc(d_change, entries, init_offset);
		// printf("d_change address: %p\n", d_change);
		init_offset += durable_alloc(d_location, entries, init_offset);
		// printf("d_location address: %p\n", d_location);
		init_offset += durable_alloc(d_group, groups, init_offset);
		// printf("d_group address: %p\n", d_group);
		entry_range = new range(entries, 0, nullptr);
		group_range = new range(groups, 0, nullptr);
		current_group = nullptr; // No open group
		log_lock = new locallock::ticket_lock();
		return init_offset - offset;
	}

	void undo_log::record_original(location_t location, char *original_data) {
		assert(("The location shouldn't be in the open group.",
			current_group == nullptr || entry_lookup.count(location) == 0));
		if (entry_range->is_full() || group_range->is_full()) {
			// commit a group
			// TODO: handle case when all entries are used by single (open) group
			group_range->inc_start();
			entry_range->inc_start(max_group_size); // Because groups are currently only closed at max size.
		}
		if (entry_lookup.size() >= max_group_size) {
			assert(("Groups should never become bigger than the max size.",
				entry_lookup.size() == max_group_size));
			// group reached max size, close it
			entry_lookup.clear();
			delete current_group; // TODO: Put in a closed queue instead
			current_group = nullptr;
		}
		if (current_group == nullptr) {
			// No open group, open one
			current_group = new group<location_t>(
				entries,
				entry_range->get_end(), // Start at the next entry
				&d_group[group_range->get_end()] // Usable as there is at least one free group slot
			);
			// PM FENCE
			group_range->inc_end(); // Include newly reset group in group buffer
		}
		// Get next free entry index (at least one free slot has been ensured)
		size_t idx = entry_range->get_end();
		assert(("The end of the global entry range should match that of the current group.",
			idx == current_group->entry_range.get_end()));
		// Persistently update group data
		d_original[idx].copy_data(original_data);
		d_change[idx].reset();
		d_location[idx] = location;
		// PM FENCE
		// Persistently expand group to include new data
		current_group->entry_range.inc_end();
		// Adjust volatile structures
		entry_lookup[location] = idx;
		entry_range->inc_end();
	}

	void undo_log::record_changes(location_t location, char *modified_data, char *original_data) {
		std::lock_guard<locallock::ticket_lock> lock(*log_lock);
		size_t idx;
		try {
			if (current_group == nullptr)
				throw std::out_of_range("No open group to in which to record changes.");
			idx = entry_lookup.at(location);
		} catch (std::out_of_range &e) {
			// Either due to lack of current group or the entry is new.
			record_original(location, original_data);
			idx = entry_lookup.at(location);
		}
		d_change[idx].update(modified_data, original_data);
	}

}