#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <sys/mman.h>
#include <unordered_map>

#include "../../synchronization/intranode/ticket_lock.hpp"
#include "../backend.hpp"
#include "virtual_memory/virtual_memory.hpp"

#include "persistence.hpp"

#define ROUND_UP_DIV(n, d) ( ((n)-1)/(d) + 1 )
#define ROUND_UP_ALIGN(n, a) ( ROUND_UP_DIV(n, a) * a )
#define FOR_RANGE_COND(type, var, from, to, cond) for (type var = from ; (cond) && var < to ; ++var)
#define FOR_RANGE(type, var, from, to) FOR_RANGE_COND(type, var, from, to, true)

// namespace argo::locallock {

// 	class ticket_lock_scope {
// 	private:
// 		ticket_lock *lock;
// 	public:
// 		ticket_lock_scope(ticket_lock *lock) : lock(lock) {
// 			this->lock->lock();
// 		}
// 		~ticket_lock_scope() {
// 			this->lock->unlock();
// 		}
// 	};

// }

namespace argo::backend::persistence {

	// template <
	// 	typename location_t,
	// 	size_t entry_size,
	// 	size_t dirty_unit
	// >
	// class durable {
	// public:
	// 	/** @brief Container of original data to use for recovery.
	// 	 * This data is generally only ever writen during normal operation
	// 	 * but may be used to find modifications. Usually another reference
	// 	 * is preferred, however.
	// 	*/
	// 	struct original_t {
	// 		unsigned char data[entry_size];
	// 	};

	// 	/** @brief Container of dirty bits for the correspoinding original data.
	// 	 * A bit being one (1) means that the corresponding dirty unit
	// 	 * in the original data needs to be reverted on recovery.
	// 	 * This data is only ever update during normal operation
	// 	 * (i.e., individual 0's becomes 1's or the entire map is reset).
	// 	*/
	// 	struct change_t {
	// 		using change_map_t = uint64_t;
	// 		static const size_t cm_flags_per_index = 64;
	// 		static const size_t change_map_length = ROUND_UP_DIV(entry_size, dirty_unit*cm_flags_per_index);
	// 		change_map_t map[change_map_length];
	// 	};

	// 	/** @brief An entry with pointers to the original data, changes and argo location. */
	// 	struct entry_t {
	// 		/** @brief Identifier of the data location monitored by this entry. */
	// 		location_t location;
	// 		/** @brief Container of original, unmodified data. */
	// 		original_t *original;
	// 		/** @brief Container of dirty bits. */
	// 		change_t *change;
	// 	};

	// 	struct group_t {
	// 		/** @brief Pointer to either end of the entries in use by the group. */
	// 		entry_t *first_entry, *last_entry;
	// 	};

	// 	struct log_t {
	// 		/** @brief Pointer to either end of the groups in use by the log. */
	// 		group_t *first_group, *last_group;
	// 	};
		
	// 	struct index_t {
	// 		/** @brief Self reference to the start of the durable log. */
	// 		index_t *data_start;
	// 		unsigned char *data_end;
	// 		log_t *log;
	// 		group_t *group_start, *group_end;
	// 		entry_t *entry_start, *entry_end;
	// 		original_t *original_start, *original_end;
	// 		change_t *change_start, *change_end;
	// 	};

	// 	index_t *index;

	// 	template <typename T, typename U>
	// 	static inline size_t place_element(
	// 		unsigned char *start,
	// 		size_t element_size,
	// 		size_t element_copies,
	// 		size_t alignment = entry_size,
	// 		T **start_ptr = (void**) nullptr,
	// 		U **end_ptr = (void**) nullptr
	// 	) {
	// 		if (start_ptr != nullptr)
	// 			*start_ptr = reinterpret_cast<T*>(start);
	// 		if (end_ptr != nullptr)
	// 			*end_ptr = reinterpret_cast<U*>(start + element_size*element_copies);
	// 		return ROUND_UP_ALIGN(element_size*element_copies, alignment);
	// 	}

	// 	static inline size_t place_elements(
	// 		size_t groups, size_t entries,
	// 		size_t alignment = entry_size,
	// 		unsigned char *start = nullptr,
	// 		index_t **index_ptr = nullptr
	// 	) {
	// 		unsigned char *current_ptr = start;
			
	// 		current_ptr += place_element(current_ptr, sizeof(index_t), 1, alignment, index_ptr, (void**) nullptr);
	// 		current_ptr += place_element(current_ptr, sizeof(log_t), 1, alignment,
	// 			index_ptr ? &((*index_ptr)->log) : nullptr, (void**) nullptr);

	// 		current_ptr += place_element(current_ptr, sizeof(group_t), groups, alignment,
	// 			index_ptr ? &((*index_ptr)->group_start) : nullptr,
	// 			index_ptr ? &((*index_ptr)->group_end) : nullptr);

	// 		current_ptr += place_element(current_ptr, sizeof(entry_t), entries, alignment,
	// 			index_ptr ? &((*index_ptr)->entry_start) : nullptr,
	// 			index_ptr ? &((*index_ptr)->entry_end) : nullptr);
	// 		current_ptr += place_element(current_ptr, sizeof(original_t), entries, alignment,
	// 			index_ptr ? &((*index_ptr)->original_start) : nullptr,
	// 			index_ptr ? &((*index_ptr)->original_end) : nullptr);
	// 		current_ptr += place_element(current_ptr, sizeof(change_t), entries, alignment,
	// 			index_ptr ? &((*index_ptr)->change_start) : nullptr,
	// 			index_ptr ? &((*index_ptr)->change_end) : nullptr);

	// 		if (index_ptr != nullptr) {
	// 			(*index_ptr)->data_start = *index_ptr;
	// 			(*index_ptr)->data_end = current_ptr;
	// 		}

	// 		return static_cast<size_t>(current_ptr - start);
	// 	}

	// 	static size_t mem_size(size_t groups, size_t entries, size_t alignment = entry_size) {
	// 		// return
	// 		// 	ROUND_UP_ALIGN(sizeof(index_t), alignment) +
	// 		// 	ROUND_UP_ALIGN(sizeof(log_t), alignment) +
	// 		// 	ROUND_UP_ALIGN(sizeof(group_t)*groups, alignment) +
	// 		// 	ROUND_UP_ALIGN(sizeof(entry_t)*entries, alignment) +
	// 		// 	ROUND_UP_ALIGN(sizeof(original_t)*entries, alignment) +
	// 		// 	ROUND_UP_ALIGN(sizeof(change_t)*entries, alignment);
	// 		return place_elements(groups, entries, alignment);
	// 	}

	// 	durable(size_t offset, size_t groups, size_t entries, size_t alignment = entry_size) {
	// 		size_t durable_size = mem_size(groups, entries, alignment);
	// 		unsigned char *durable_mem = reinterpret_cast<unsigned char*>(argo::virtual_memory::allocate_mappable(alignment, durable_size));
	// 		argo::virtual_memory::map_memory(durable_mem, durable_size, offset, PROT_READ|PROT_WRITE, argo::virtual_memory::memory_type::nvm);

	// 		// durable_mem += place_element(durable_mem, sizeof(index_t), 1, alignment, &(this->index));
	// 		// durable_mem += place_element(durable_mem, sizeof(log_t), 1, alignment, &(this->index->log));

	// 		// durable_mem += place_element(durable_mem, sizeof(group_t), groups, alignment,
	// 		// 	&(this->index->group_start), &(this->index->group_end));

	// 		// durable_mem += place_element(durable_mem, sizeof(entry_t), entries, alignment,
	// 		// 	&(this->index->entry_start), &(this->index->entry_end));
	// 		// durable_mem += place_element(durable_mem, sizeof(original_t), entries, alignment,
	// 		// 	&(this->index->original_start), &(this->index->original_end));
	// 		// durable_mem += place_element(durable_mem, sizeof(change_t), entries, alignment,
	// 		// 	&(this->index->change_start), &(this->index->change_end));

	// 		// this->index->data_start = this->index;
	// 		// this->index->data_end = durable_mem;

	// 		place_elements(groups, entries, alignment, durable_mem, &index);
	// 	}

	// 	size_t mem_size() {
	// 		return static_cast<size_t>(this->index->data_end - reinterpret_cast<unsigned char*>(this->index->data_start));
	// 	}

	// 	static durable recover(unsigned char* index) {
	// 		durable rec = new durable();
	// 		rec->index = index;
	// 		return rec;
	// 	}
	// };

	// durable<unsigned char*, 4096, 1> *durable_log;

	size_t initialize(size_t offset, size_t alignment) {
		// durable_log = new durable<unsigned char*, 4096, 1>(offset, 64, 64*1024, alignment);
		// return durable_log->mem_size();
		return 0; // TODO: fix
	}

	// @todo Why am I not using std::lock_guard ? 
	template<typename lock_t>
	class scope_lock {
	private:
		lock_t *lock_ptr;
		bool locked = false;
	public:
		scope_lock(lock_t *lock_ptr, bool lock_now = true)
		: lock_ptr(lock_ptr) {
			if (lock_now) lock();
		}
		~scope_lock() { unlock(); }
		void lock() {
			if (!locked) {
				this->lock_ptr->lock();
				locked = true;
			}
		}
		void unlock() {
			if (locked) {
				locked = false;
				this->lock_ptr->unlock();
			}
		}
		bool is_locked() { return locked; }
	};

	/** @brief Abstract base class for a log. */
	class log_base {
	public:
		/**
		 * @brief Clear the log.
		 * Atomically and persitently invalidates the log and then removes
		 * its content.
		 */
		virtual void commit() = 0;
	};

	/** @brief Abstract base class for an undo log.
	 * @tparam location_t Identification type describing a data blocks location.
	 */
	template<typename location_t>
	class undo_log_base : public log_base {
	public:
		/** @brief Record original data to track in the log.
		 * The original data is marked as clean, meaning the log is unaware of
		 * any actual changes.
		 * @note The size of the data block is determined by the class.
		 * @param location Identifier of the data location.
		 * @param original_data Pointer to unmodified data.
		 */
		virtual void record_original(location_t location, char *original_data) = 0;

		/** @brief Record changes to preserve in the log.
		 * Any differences in the modified data, compared to the original,
		 * are marked as dirty in the log.
		 * @note No data reverts from dirty to clean.
		 * @note The granularity of dirty data is determined by the class.
		 * @note The size of the data block is determined by the class.
		 * @param location Identifier of the data location.
		 * @param modified_data Pointer to the modified version of the data.
		 * @param original_data Optional pointer to unmodified data.
		 *                      Only observes differences to this data
		 *                      instead of the stored original.
		 */
		virtual void record_changes(location_t location, char *modified_data, char *original_data = nullptr) = 0;
	};

	/* We need a way to enter an entry, it cannot stay in the pagecopy since
	 * it may be overwritten by a new cache entry (when written).
	 * The entire page needs to be stored in the undo log along with a bitmap of dirty bytes.
	 * The bitmap has to be amended with more dirty bytes. (I.e. random access to entries.)
	 * It should be possible to commit (and clear) the log in a singe instant.
	 * A restore function should possibly exist but does not have to be implemented right now.
	 */
	/** @brief Implementation of an undo log.
	 * @tparam location_t Identification type describing a data blocks location.
	 * @tparam entry_size Size (in bytes) of data entries.
	 * @tparam dirty_unit Size (in bytes) each dirty bit cover.
	 * @note @p dirty_unit must divide @p entry_size.
	 */
	template<
		typename location_t,
		size_t entry_size,
		size_t dirty_unit
	>
	class undo_log : public undo_log_base<location_t> {

	public:

		/** @brief Container of original data to use for recovery.
		 * This data is generally only ever writen during normal operation
		 * but may be used to find modifications. Usually another reference
		 * is preferred, however.
		*/
		struct durable_original {

			/** @brief Container of original, unmodified data. */
			unsigned char data[entry_size];

			/** @brief Copy data into the original container.
			 * @param source Pointer to the original data to copy from. Must be @c entry_size in length.
			*/
			void copy_data(unsigned char *source) {
				memcpy(this->data, source, entry_size);
			}

		};

		/** @brief Container of dirty bits for the correspoinding original data.
		 * A bit being one (1) means that the corresponding dirty unit
		 * in the original data needs to be reverted on recovery.
		 * This data is only ever update during normal operation
		 * (i.e., individual 0's becomes 1's or the entire map is reset).
		*/
		struct durable_change {

			using change_map_type = uint64_t;
			static const size_t cm_flags_per_index = 64;
			static const size_t change_map_length = ROUND_UP_DIV(entry_size, dirty_unit*cm_flags_per_index);
			static const size_t change_map_bytes = change_map_length*sizeof(change_map_type);

			/** @brief Container of dirty bits. */
			change_map_type map[change_map_length];

			/** @brief Set dirty bits to 0. */
			void reset() {
				memset(map, 0, change_map_bytes);
			}

			/** @brief Compare @p modified and @p original and set corresponding dirty bits.
			 * @param modified Pointer to modified data (e.g. in the ArgoDSM cache).
			 * @param original Pointer to original data (e.g. in the ArgoDSM write buffer).
			 * @note Avoid using data in the persistent log for the original. This is mainly for performance but may also affect correctness.
			*/
			void update(unsigned char *modified, unsigned char *original) {
				size_t data_index = 0;
				FOR_RANGE_COND(size_t, cm_index, 0, change_map_length, data_index < entry_size) {
					change_map_type cm_entry = 0;
					FOR_RANGE_COND(size_t, cm_subindex, 0, cm_flags_per_index, data_index < entry_size) {
						change_map_type cm_subentry = 0;
						FOR_RANGE(size_t, dirty_index, 0, dirty_unit) {
							// Cond (data_index < entry_size) not needed since dirty_unit divides entry size.
							if (original_data[data_index] != modified_data[data_index])
								cm_subentry = 1; // The dirty unit has changed.
							++data_index;
						}
						cm_entry |= cm_subentry << cm_subindex;
					}
					if (cm_entry != 0)
						map[cm_index] |= cm_entry;
				}
			}

		};

		/** @brief An entry with a reference to the original argo location. */
		struct durable_entry {
			/** @brief Identifier of the data location monitored by this entry. */
			location_t location;
		};

		template<typename T>
		struct durable_array {
			/** @brief Start of the array. */
			T* start;
			/** @brief Size of the array. */
			size_t size;
			/** @brief End of the array.
			 * @returns Address of (one past) the end of the array based on the @c start address and @c size.
			*/
			durable_array() = default;
			durable_array(T* start, size_t size) : start(start), size(size) {}
			inline T* end() { return start + size; }
			T* addr(size_t index) {
				if (index >= size) throw std::out_of_range;
				return start + index;
			}
		};

		struct durable_range {
			/** @brief Start of the range (inclusive). */
			size_t start;
			/** @brief End of the range (exclusive). */
			size_t end;
		};

		struct durable_group : public durable_range {};
		struct durable_log : public durable_range {};
		
		/** @brief Pointers outlining the structure of the durable part of the log.
		 * @note Everything in this struct is decided statically or at launch time
		 * and is therefore redundant assuming the application and launch configuration
		 * can be recovered by other means.
		 * This structure could be of some use for forensics, however.
		 */
		struct durable_index {
			/** @brief Self-reference pointer to the array containing the durable log. */
			durable_array<unsigned char> data;

			/** @brief Self-reference pointer to this index. */
			durable_index *index;

			/** @brief Pointer to the durable log. */
			durable_log *log;

			/** @brief Pointer and size of the durable log group array. */
			durable_array<durable_group> group;

			/** @brief Pointer and size of the durable log entry array. */
			durable_array<durable_entry> entry;
			/** @brief Pointer and size of the durable log original array. */
			durable_array<durable_original> original;
			/** @brief Pointer and size of the durable log change array. */
			durable_array<durable_change> change;

			static size_t mem_size(size_t groups, size_t entries, size_t alignment) {
				// size, assuming the start address is aligned
				return
					ROUND_UP_ALIGN( sizeof(durable_index), std::max(alignment, alignof(durable_index)) ) +
					ROUND_UP_ALIGN( sizeof(durable_log),   std::max(alignment, alignof(durable_log))   ) +
					ROUND_UP_ALIGN( sizeof(durable_group) * groups, std::max(alignment, alignof(durable_group)) ) +
					ROUND_UP_ALIGN( sizeof(durable_entry)    * entries, std::max(alignment, alignof(durable_entry))    ) +
					ROUND_UP_ALIGN( sizeof(durable_original) * entries, std::max(alignment, alignof(durable_original)) ) +
					ROUND_UP_ALIGN( sizeof(durable_change)   * entries, std::max(alignment, alignof(durable_change))   );
			}

			template<typename T>
			static inline void place_element_array(
				durable_array<T> &array_ref,
				size_t element_copies,
				size_t alignment,
				unsigned char *&buf_start,
				size_t &buf_size
			) {
				if (std::align(std::max(alignment, alignof(T)), sizeof(T)*element_copies, buf_start, buf_size) == nullptr)
					throw std::bad_alloc
				array_ref.start = reinterpret_cast<T*>(buf_start);
				array_ref.size = groups;
				buf_start += sizeof(T)*element_copies;
				buf_size -= sizeof(T)*element_copies;
			}

			template<typename T>
			static inline void place_element(
				T *&ptr_ref,
				size_t alignment,
				unsigned char *&buf_start,
				size_t &buf_size
			) {
				durable_array<T> temp_arr;
				place_element_array(temp_arr, 1, alignment, buf_start, buf_size)
				ptr_ref = temp_arr.start;
			}

			static inline void place_durable_data(
				size_t groups,
				size_t entries,
				size_t alignment, // must be power of 2
				unsigned char *buf_start, // start of an unsigned char array
				size_t buf_size,
				durable_index &index_ref
			) {
				unsigned char *current_ptr = buf_start;
				size_t remaining_size = buf_size;

				index_ref.data.start = start;
				index_ref.data.size = total_size;

				place_element(index_ref.index, alignment, current_ptr, remaining_size);
				place_element(index_ref.log, alignment, current_ptr, remaining_size);

				place_element_array(index_ref.group, groups, alignment, current_ptr, remaining_size);

				place_element_array(index_ref.entry,    entries, alignment, current_ptr, remaining_size);
				place_element_array(index_ref.original, entries, alignment, current_ptr, remaining_size);
				place_element_array(index_ref.change,   entries, alignment, current_ptr, remaining_size);
			}

			// static inline size_t index_inc(size_t index_limit, size_t index, size_t offset = 1) {
			// 	if (index >= index_limit)
			// 		throw std::invalid_argument("Index is invalid");
			// 	offset = offset % index_limit;
			// 	if (offset < index_limit - index) // avoid wrap?
			// 		return index + offset;
			// 	return index - (index_limit - offset);
			// }

			// static inline size_t index_dec(size_t index_limit, size_t index, size_t offset = 1) {
			// 	if (index >= index_limit)
			// 		throw std::invalid_argument("Index is invalid");
			// 	offset = offset % index_limit;
			// 	if (offset <= index) // avoid wrap?
			// 		return index - offset;
			// 	return index + (index_limit - offset);
			// }

			// static inline size_t index_offset(size_t index_limit, size_t index, int offset) {
			// 	if (index >= index_limit)
			// 		throw std::invalid_argument("Index is invalid");
			// 	if (offset > 0)
			// 		return index_inc(index_limit, index, offset);
			// 	if (offset < 0)
			// 		return index_inc(index_limit, index, -offset);
			// 	return index;
			// }
		};

	private:

		template<typename T>
		class volatile_buffer : private durable_range {
			using durable_range::start;
			using durable_range::end;
			durable_array<T> domain;
		public:
			volatile_buffer(const durable_range &range, const durable_array<T> &domain)
			: start(range.start), end(range.end), domain(domain) {}
			volatile_buffer(const durable_array<T> &domain)
			: start(0), end(0), domain(domain) {}
			volatile_buffer(size_t start, size_t end, const durable_array<T> &domain)
			: start(start), end(end), domain(domain) {}
			volatile_buffer(size_t start, size_t end, T* start_ptr, size_t size)
			: start(start), end(end), domain(start_ptr, size) {}
			size_t inc_start() {
				if (start == end) throw std::length_error;
				size_t inc = start + 1 % domain.size;
				return start = inc;
			}
			size_t inc_end() {
				size_t inc = end + 1 % domain.size;
				if (inc == start) throw std::length_error;
				return end = inc;
			}
			size_t in_use() { return end + (end < start ? 0 : domain.size) - start; }
			T* addr_start() { return domain.addr(start); }
			T* addr_end() { return domain.addr(end); }
			size_t get_start() { return start; }
			size_t get_end() { return end; }
			size_t get_size() { return domain.size; }
			T* addr(size_t index) { return domain.addr(index); } 
		};

		// template<typename T>
		// class volatile_domain : private durable_range<T> {
		// public:
		// 	volatile_domain(T start, T end) : start(start), end(end) {}
		// 	volatile_domain(durable_range<T> &domain) : start(domain.start), end(domain.end) {}
		// 	inline size_t length() { return static_cast<size_t>(this->end - this->start); }
		// 	template<typename U>
		// 	inline U normalise(U index) {
		// 		size_t len = this->length();
		// 		index = index % len;
		// 		if (index < 0) index += len;
		// 		return index;
		// 	}
		// 	template<typename U>
		// 	inline T at(U index) { return this->start + this->normalise(index); }
		// 	inline T get_start() { return this->start; }
		// 	inline T get_end() { return this->end; }
		// };

		// template<typename T>
		// class volatile_range : private durable_range<T> {
		// 	volatile_domain<T> domain;

		// public:

		// 	volatile_range(durable_range<T> range, volatile_domain<T> domain = range)
		// 	: start(range.start), end(range.end), domain(domain) {}
		// 	volatile_range(T start, T end, T domain_start = start, T domain_end = end)
		// 	: start(start), end(end) { domain.start = domain_start ; domain.end = domain_end; }
		// 	volatile_range(T start, T end, volatile_domain<T> domain)
		// 	: start(start), end(end), domain(domain) {}
		// 	volatile_range(durable_range<T> range, T domain_start, T domain_end)
		// 	: start(range.start), end(range.end) { domain.start = domain_start ; domain.end = domain_end; }
			
		// 	inline T get_start() { return this->start; }
		// 	inline T get_end() { return this->end; }
		// 	inline T get_domain_start() { return this->domain.get_start(); }
		// 	inline T get_domain_end() { return this->domain.get_end(); }
		// };

		// durable_index *index;

		// template<typename T>
		// class volatile_domain_range : private durable_range<T> {
		// 	durable_range<T> domain;
		// 	volatile_domain_range()
		// };

		/** @brief A storage link in the undo log. */
		class volatile_entry : private durable_entry {

		private:
			// from durable_entry
			// location_t location;

			/** @brief Pointer to an index with pointers to memory segments. */
			const durable_index *index;

			/** @brief Pointer to the corresponding durable entry. */
			durable_entry *durable;

			/** @brief Pointer to container of original, unmodified data. */
			durable_original *original;

			/** @brief Pointer to container of dirty bits. */
			durable_change *change;

		public:

			/** @brief Create a new volatile entry and initialise the durable counterpart.
			 * @param index Pointer to an index with pointers to memory segments.
			 * @param entry_index Index of the durable entry to initialise.
			 * @param location Identifier of the data location.
			 * @param original_data Pointer to original, unmodified data.
			 */
			volatile_entry(const durable_index *index, size_t entry_index, location_t location, unsigned char *original_data)
			: index(index)
			, location(location) {
				// Find pointers from base pointes and entry index
				this->durable = &(index->entry.start[entry_index]);
				this->original = &(index->original.start[entry_index]);
				this->change = &(index->change.start[entry_index]);
				// Populate durable structures
				durable->location = this->location;
				this->original->copy_data(original_data);
				this->change->reset();
				pm_fence();
			}

			/** @brief Update dirty bits for new changes.
			 * Updates the dirty bits according to the difference
			 * between @p modified_data and @p original_data,
			 * alternatively the the stored original.
			 * @param modified_data Pointer to the modified version of the data.
			 * @param original_data Optional pointer to unmodified data.
			 *                      Only observed differences to this data
			 *                      instead of the stored original.
			 */
			void update_changes(unsigned char *modified_data, unsigned char *original_data = nullptr) {
				if (original_data == nullptr)
					original_data = &(this->original->data[0]);
				this->change->update(modified_data, original_data);
				pm_fence();
			}

		};

		class volatile_group : private durable_group
		{

		private:

			using durable_group::start;
			using durable_group::end;

			const durable_index *index;
			durable_group *durable;
			std::unordered_map<location_t, volatile_entry*> entry_lookup();

			/** @brief Creates a new entry for a new location.
			 * @param location Identifier of the data location.
			 * @param original_data Pointer to original, unmodified data.
			 * @param check_exist Whether to check if @p location already exists in the log.
			 * @exception Throws @c std::invalid_argument if the location already exists in the log (if checked).
			 * @return The newly created entry.
			 */
			volatile_entry *create_new_entry(
				location_t location,
				unsigned char *original_data,
				bool check_exist = true
			) {
				if (check_exist && entry_lookup->count(location) > 0)
					throw std::invalid_argument("Location already entered into the group.");
				// TODO: check if entry is available
				volatile_entry *new_entry = new volatile_entry(this->entry.end, location, original_data);
				entry_lookup.insert({location, new_entry});
				pm_fence(); // Make sure the entry is initialised before advancing index
				size_t next_index = (this->entry.end + 1) % index->entry_count;
				this->entry.end = next_index
				this->durable->entry.end = next_index;
				pm_fence(); // Make sure index is advanced before returning
				return new_entry;
			}

		public:
		
			volatile_group(const durable_index *index, size_t group_index, size_t next_entry)
			: index(index)
			, durable(&(index->group.start[group_index])) {
				this->durable->start = this->start = next_entry;
				this->durable->end = this->end = next_entry;
				this->entry_lookup = new std::unordered_map<location_t, volatile_entry*>();
			}

			~volatile_group() {
				for (const std::pair<location_t, volatile_entry*> entry : entry_lookup)
					delete entry.second;
			}

			/** @copydoc undo_log_base::record_original()
			 * @exception Throws @c std::invalid_argument if the location already
			 *            exists in the log.
			 */
			void record_original(location_t location, unsigned char *original_data) {
				// scope_lock<locallock::ticket_lock> sc(log_lock);
				create_new_entry(location, original_data);
			}

			/** @copydoc undo_log_base::record_changes()
			 * @exception Throws @c std::invalid_argument if the location doesn't
			 *            already exist in the log and no original data is provided.
			 */
			void record_changes(
				location_t location,
				unsigned char *modified_data,
				unsigned char *original_data = nullptr
			) {
				// scope_lock<locallock::ticket_lock> sc(log_lock);
				volatile_entry *group_entry;
				try {
					group_entry = entry_lookup->at(location);
				} catch (std::out_of_range &e) {
					if (original_data == nullptr)
						throw std::invalid_argument("Location doesn't exist in the log.");
					else
						group_entry = create_new_entry(location, original_data, false);
				}
				group_entry->update_changes(modified_data, original_data);
				pm_fence();
			}

		};

		class volatile_log : private durable_log
		{

		private:

			using durable_log::start; // index of group
			using durable_log::end; // index (one past) of group

			const durable_index *index;
			durable_log *durable;
			durable_range entries;

		public:
		
			volatile_log(size_t next_group) {
				this->durable = index->log;
				this->durable->group_start = this->group_start = next_group;
				this->durable->group_end = this->group_end = next_group;
				this->group_lookup = new std::unordered_map<location_t, volatile_group*>();
			}

			~volatile_log() {
				for (const std::pair<location_t, volatile_group*> group : this->group_lookup) {
					delete group.second;
				}
				delete this->group_lookup;
			}

		private:

			/** @brief Creates a new entry for a new location.
			 * @param location Identifier of the data location.
			 * @param original_data Pointer to original, unmodified data.
			 * @param check_exist Whether to check if @p location already exists in the log.
			 * @exception Throws @c std::invalid_argument if the location already exists in the log (if checked).
			 * @return The newly created log entry.
			 */
			link *create_new_group() {
				// TODO: check if group is available
				volatile_group *new_group =
					new volatile_group(this->end, );
				log_entries_head = new_entry;
				entries->insert({this->entry_end, new_entry});
				entry_lookup->insert({location, new_entry});
				pm_fence(); // Make sure the entry is initialised before advancing index
				this->durable->entry_end =
					this->entry_end =
					(this->entry_end + 1) % index->entry_count;
				pm_fence(); // Make sure index is advanced before returning
				return new_entry;
			}

		public:

			/** @copydoc undo_log_base::record_original()
			 * @exception Throws @c std::invalid_argument if the location already
			 *            exists in the log.
			 */
			void record_original(location_t location, unsigned char *original_data) {
				// scope_lock<locallock::ticket_lock> sc(log_lock);
				create_new_group(location, original_data);
			}

			/** @copydoc undo_log_base::record_changes()
			 * @exception Throws @c std::invalid_argument if the location doesn't
			 *            already exist in the log and no original data is provided.
			 */
			void record_changes(
				location_t location,
				unsigned char *modified_data,
				unsigned char *original_data = nullptr
			) {
				// scope_lock<locallock::ticket_lock> sc(log_lock);
				volatile_entry *group_entry;
				try {
					group_entry = entry_lookup->at(location);
				} catch (std::out_of_range &e) {
					if (original_data == nullptr)
						throw std::invalid_argument("Location doesn't exist in the log.");
					else
						group_entry = create_new_entry(location, original_data, false);
				}
				group_entry->update_changes(modified_data, original_data);
				pm_fence();
			}

		};

		/** @brief Links of the log.
		 * A null pointer indicates that the log is committed.
		 */
		durable<location_t, entry_size, dirty_unit> *durable_log;

		durable<location_t, entry_size, dirty_unit>::index_t index;
		durable<location_t, entry_size, dirty_unit>::group_t index;
		durable<location_t, entry_size, dirty_unit>::entry_t index;

		/** @brief Direct lookup for log links givent its location.
		 * @note This lookup is not persistent, it is only an access
		 *       optimisation for log modification.
		 */
		std::unordered_map<location_t, link*> *log_lookup;

		/** @brief Handling exclusive access for the structure. */
		locallock::ticket_lock *log_lock;

		/** @todo ensure pm writes are properly committed */
		static void pm_fence() {}

		/** @brief Creates a new log entry for a new location.
		 * @param location Identifier of the data location.
		 * @param original_data Pointer to original, unmodified data.
		 * @param check_exist Whether to check if @p location already exists in the log.
		 * @exception Throws @c std::invalid_argument if the location already exists (if checked) in the log.
		 * @return The newly created log entry.
		 */
		link *create_new_entry(location_t location, char *original_data, bool check_exist = true) {
			if (check_exist && log_lookup->count(location) > 0)
				throw std::invalid_argument("Location already entered into the log.");
			link *log_entry = new link(log_entries_head, location, original_data);
			pm_fence();
			log_entries_head = log_entry;
			log_lookup->insert({location, log_entry});
			pm_fence();
			return log_entry;
		}

	public:

		undo_log(durable<location_t, entry_size, dirty_unit> *durable_log)
		: durable_log(durable_log)
		{
			memcpy(this->index, durable_log->index, sizeof(durable<location_t, entry_size, dirty_unit>::index_t));
			log_lookup = new std::unordered_map<location_t, link*>();
			log_lock = new locallock::ticket_lock();
		}

		~undo_log() {
			commit();
			delete log_lookup;
			delete log_lock;
		}

		/** @copydoc undo_log_base::record_original()
		 * @exception Throws @c std::invalid_argument if the location already
		 *            exists in the log.
		 */
		void record_original(location_t location, char *original_data) {
			// locallock::ticket_lock_scope scope_lock(log_lock);
			scope_lock<locallock::ticket_lock> sc(log_lock);
			create_new_entry(location, original_data);
		}

		/** @copydoc undo_log_base::record_changes()
		 * @exception Throws @c std::invalid_argument if the location doesn't
		 *            already exist in the log and no original data is provided.
		 */
		void record_changes(location_t location, char *modified_data, char *original_data = nullptr) {
			// locallock::ticket_lock_scope scope_lock(log_lock);
			scope_lock<locallock::ticket_lock> sc(log_lock);
			link *log_entry;
			try {
				log_entry = log_lookup->at(location);
			} catch (std::out_of_range &e) {
				if (original_data == nullptr)
					throw std::invalid_argument("Location doesn't exist in the log.");
				else
					log_entry = create_new_entry(location, original_data, false);
			}
			log_entry->update_changes(modified_data, original_data);
			pm_fence();
		}

		/** @copydoc log_base::commit() */
		void commit() {
			link *head;
			{
				// locallock::ticket_lock_scope scope_lock(log_lock);
				scope_lock<locallock::ticket_lock> sc(log_lock);
				pm_fence();
				head = log_entries_head;
				log_entries_head = nullptr; // effective commit
				pm_fence();
				// cleanup
				log_lookup->clear(); // after pm_fence, as the lookup is not persistent
			}
			link::delete_list(head); // only this call has a reference to these invalidated entries
		}

	};

	/* control structure, volatile
	/** @brief Arbiter for restore point creation.
	 * A restore point is created when commiting and persistence log. This
	 * class coordinates participating actors to create opportuinities to
	 * create restore points. To function properly, all actors that use the
	 * persistence log attached to an arbiter has to get a tracker from said
	 * arbiter and indicate when it is and is not ready to create a restore
	 * point. Actors should take care not to stall without allowing restore
	 * point creation and must eventually (prefereably with sufficient
	 * frequency) allow restore point creation.
	 */
	class restore_arbiter {

	private:

		/** @brief Indicates that a restore point creation is underway. */
		std::atomic_bool restore_in_progress;
		/** @brief Count of trackers that prevents a restore point to be created. */
		std::atomic_size_t prohibiting_trackers;
		/** @brief Pointer to attached persistence log. */
		log_base *log;

	public:

		/** @copydoc restore_arbiter
		 * @param log The persistence log to attach to the arbiter.
		 */
		restore_arbiter(log_base *log)
		: restore_in_progress(false)
		, prohibiting_trackers(0)
		, log(log) {}

		~restore_arbiter() {}

		/** @brief Stalls until a restore point creation has completed.
		 * @note A new restore point creation could start right after the return.
		 */
		void wait_restore() {
			while (restore_in_progress) {}
		}

	private:

		/** @brief Remove a tracker as prohibiting.
		 * This method should be used when an actor may stall for a longer
		 * period while a restore point is created.
		 */
		void allow_restore() {
			prohibiting_trackers--;
		}

		/** @brief Add a tracker as prohibiting.
		 * This method should be used when when it is no longer acceptable for
		 * an actor to experience a restore point creation. The method will
		 * stall until no restore point creation is in progress.
		 */
		void prohibit_restore() {
			// Wait for any restore to complete.
			wait_restore();
			// A new restore could be initiated here...
			prohibiting_trackers++;
			// ...if so it should be joined.
			join_restore(); // Mutual tail-recurson, compiler's tail-call optimisation should deal with it.
			// Why join a new restore?
			// If not, the restore could interfere with the following SFR.
			// If just waiting for the restore to complete, a deadlock could occur.
		}

		/** @brief Checks if any restore point is being created and allows it to proceed.
		 * If actors doesn't periodically allow retore points, this method
		 * should be called when it is acceptable to create a resore point.
		 */
		void join_restore() {
			if (restore_in_progress) {
				allow_restore();
				prohibit_restore();
			}
		}

	public:

		/** @brief Starts a restore point creation if one is not underway.
		 * @return @c false if a restore point creation was already in progres,
		 *         otherwise @c true once the restore point is completed.
		 * @note May not be called by an actor prohibiting restore points,
		 *       this would result in a deadlock.
		 */
		bool try_make_restore() {
			bool expected = false;
			if (!restore_in_progress.compare_exchange_strong(expected, true))
				return false; // Did not make a restore, one was in progress.
			// A restore was not in progress, but now it has been started.
			// (1/3) Wait for other threads to allow.
			while (prohibiting_trackers > 0) {}
			// (2/3) Perform restore point creation (i.e., flush write buffer and commit log).
			argo::backend::release();
			log->commit();
			// (3/3) Allow other threads to continue.
			restore_in_progress = false;
			return true; // Made a restore.
		}

		/** @brief Creates a restore point.
		 * This method creates a restore point, alternatively waits for the
		 * one underway to complete.
		 */
		void make_restore() {
			if (!try_make_restore())
				wait_restore();
		}

		/** @brief Tracker used to interact with a @c restore_arbiter.
		 * This class represents an actor or thread that wants to interact with
		 * the arbiter (and thus uses the arbiter's persistence log).
		 */
		class tracker {

		private:

			/** @brief The @c restore arbiter attached to the @c tracker. */
			restore_arbiter *arbiter;
			/** @brief Indicates whether the tracker is prohibiring restore points. */
			bool prohibiting = false;

		protected:

			friend class restore_arbiter;

			/** @copybrief tracker
			 * @param arbiter The arbiter the tracker should be attached to.
			 * @param prohibit Whether to prohibit restore points immediately.
			 */
			tracker(restore_arbiter *arbiter, bool prohibit = true)
			: arbiter(arbiter) {
				if (prohibit) {
					prohibit_restore();
				}
			}

		public:

			~tracker() { allow_restore(); }

			/** @brief Wether the tracker is prohibiting.
			 * @return Wether the tracker is prohibiting.
			 */
			bool is_prohibiting() { return prohibiting; }

			/** @brief Removes the tracker as prohibiting with the arbiter. */
			void allow_restore() {
				if (prohibiting)
					arbiter->allow_restore();
				prohibiting = false;
			}

			/** @brief Adds the tracker as prohibiting with the arbiter.
			 * @note Will stall until no restore point creation is in progress.
			 */
			void prohibit_restore() {
				if (!prohibiting)
					arbiter->prohibit_restore();
				prohibiting = true;
			}

			/** @brief Join any restore point creation underway with the arbiter.
			 * @note Will stall until no restore point creation is in progress.
			 */
			void join_restore() {
				if (prohibiting)
					arbiter->join_restore();
				// The tracker will remain prohibiting afterwards.
			}

			/** @brief Starts or joins a restore point creation with the arbiter.
			 * @note Will stall until the restore point creation is done.
			 * @note Will automatically allow a restore before starting it and
			 *       disallow after if that was the original state.
			 */
			void make_restore() {
				bool was_prohibiting = prohibiting;
				allow_restore();
				arbiter->make_restore();
				if (was_prohibiting)
					prohibit_restore();
			}

		};

		/** @brief Get a tracker connected to this arbiter.
		 * @param prohibit Whether to prohibit restore points immediately.
		 * @return Pointer to a tracker connected to this arbiter.
		 */
		tracker *create_tracker(bool prohibit = true) {
			return new tracker(this, prohibit);
		}

	};

}