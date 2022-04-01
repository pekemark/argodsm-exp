//#include <atomic>
#include <bitset>
#include <cstdlib>
#include <cstring>
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
		init_offset += durable_alloc(d_loc, entries, init_offset);
		// printf("d_loc address: %p\n", d_loc);
		return init_offset - offset;
	}

	void undo_log::record_original(location_t location, char *original_data) {
		size_t idx;
		try {
			idx = entry_lookup.at(location);
		} catch (std::out_of_range &e) {
			idx = next_entry;
			next_entry = (next_entry + 1) % entries;
			entry_lookup.erase(d_loc[idx]);
			entry_lookup[location] = idx;
		}
		d_original[idx].copy_data(original_data);
		d_change[idx].reset();
		d_loc[idx] = location;
	}

	void undo_log::record_changes(location_t location, char *modified_data, char *original_data) {
		size_t idx;
		try {
			idx = entry_lookup.at(location);
		} catch (std::out_of_range &e) {
			record_original(location, original_data);
			idx = entry_lookup.at(location);
		}
		d_change[idx].update(modified_data, original_data);
	}

}