//#include <atomic>
#include <bitset>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <mutex>
#include <sys/mman.h>
#include <unordered_map>

#include <mpi.h>
#include <semaphore.h>
extern sem_t ibsem;

#include "../../synchronization/intranode/ticket_lock.hpp"
#include "../backend.hpp"
#include "virtual_memory/virtual_memory.hpp"

#include "persistence.hpp"

// argo::backend::persistence::undo_log persistence_log; // Currently declared in swdsm.cpp
argo::backend::persistence::apb_arbiter persistence_arbiter(&persistence_log);
argo::backend::persistence::thread_registry persistence_registry(&persistence_arbiter);

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
		size_t inline get_last(size_t size) { return (end + (size-1)) % size; }

		bool inline is_empty() { return start == end; }
		bool inline is_full(size_t size) { return !is_empty() && get_start(size) == get_end(size); }

		size_t get_use(size_t size) {
			if (is_empty()) return 0;
			return ((get_end(size) + size) - get_start(size)) % size;
		}

	};

	class range {

		durable_range idx;
		const size_t size;

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

		size_t inline get_size() { return size; }

		size_t inline get_start() { return idx.get_start(size); }
		size_t inline get_end() { return idx.get_end(size); }
		size_t inline get_last() { return idx.get_last(size); }

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
	struct durable_lock	{
		lock_repr::lock_repr_type old_field;
		lock_repr::lock_repr_type new_field;
		location_t location;
	};

	template<typename T>
	struct durable_list_node {
		size_t next;
		T data;
	};

	template<typename Key, typename T>
	class list {
		struct node {
			size_t prev;
			size_t next;
			Key key;
		};
		size_t *const d_head;
		durable_list_node<T> *const d_node;
		const size_t num_nodes; // how many allocated nodes there are, also used as a null value.
		std::unordered_map<Key, size_t> list_lookup;
		std::unordered_map<size_t, node> ptr;
		node edge; // next == head, prev == tail
	public:
		list(size_t *d_head, durable_list_node<T> *d_nodes, size_t num_nodes)
		: d_head(d_head), d_node(d_nodes), num_nodes(num_nodes) {
			*d_head = num_nodes;
			edge.next = num_nodes;
			edge.prev = num_nodes;
		}
		void push_front(Key key, size_t idx) {
			if (!list_lookup.insert({key, idx}).second)
				throw std::invalid_argument("key already exists in list.");
			if (!ptr.insert({idx, {.prev = num_nodes, .next = edge.next, .key = key}}).second)
				throw std::invalid_argument("idx already exists in list.");
			d_node[idx].next = edge.next;
			// PM FENCE
			*d_head = idx;
			// PM FENCE
			edge.next = idx;
			if (edge.prev == num_nodes) edge.prev = idx; // Set tail if list is empty
		}
		size_t front_idx() { return edge.next; }
		T *front() { return &d_node[front_idx()].data; }
		size_t next_idx(size_t idx) { return ptr.at(idx).next; }
		bool is_end(size_t idx) { return idx == num_nodes; }
		bool empty() { return is_end(front_idx()); }
		void unlink_idx(size_t idx) {
			// Find node to unlink
			node &unode = ptr.at(idx);
			// Persistenly unlink node
			if (unode.prev == num_nodes) { // If head
				*d_head = unode.next;
				// PM FENCE
			} else {
				d_node[unode.prev].next = unode.next;
				// PM FENCE
			}
			// Unlink in volatile list (double-ended)
			if (unode.prev == num_nodes) edge.next = unode.next;
			else           ptr.at(unode.prev).next = unode.next;
			if (unode.next == num_nodes) edge.prev = unode.prev;
			else           ptr.at(unode.next).prev = unode.prev;
			// Remove volatile node
			list_lookup.erase(unode.key);
			ptr.erase(idx);
			// Note: Durable node idx is intetionally not cleared nor returned to free pool.
		}
		void pop_front() {
			if (empty()) return;
			unlink_idx(edge.next);
		}
		bool contains(Key key) { return list_lookup.count(key) > 0; }
		size_t lookup_idx(Key key) { return list_lookup.at(key); }
		T *lookup(Key key) { return &d_node[list_lookup.at(key)].data; }
	};

	struct durable_group {
		durable_range entry_range;
		size_t lock_head;
		size_t unlock_head;
	};

	template<typename location_t>
	struct group {
		std::unordered_map<location_t, size_t> entry_lookup;
		std::unordered_map<location_t, size_t> lock_lookup;
		range entry_range;
		list<location_t, durable_lock<location_t>> lock_list;
		list<location_t, durable_lock<location_t>> unlock_list;
		group(
			size_t entry_buffer_size, size_t entry_buffer_start,
			size_t lock_pool_size, durable_list_node<durable_lock<location_t>> *lock_pool_start,
			durable_group *d_group)
		: entry_range(entry_buffer_size, entry_buffer_start, &d_group->entry_range)
		, lock_list(&d_group->lock_head, lock_pool_start, lock_pool_size)
		, unlock_list(&d_group->unlock_head, lock_pool_start, lock_pool_size)
		{}
	};

	struct durable_log {
		durable_range group_range;
	};

	template<typename T>
	class mpi_atomic_array {
		static_assert(sizeof(T) == 4 || sizeof(T) == 8,
			"The size of the datatype must be 4 or 8 bytes.");
		const MPI_Datatype mpi_type = (sizeof(T) == 4 ? MPI_INT32_T : MPI_INT64_T);
		T *base;
		size_t elems;
		MPI_Win array_window;
	public:
		mpi_atomic_array(T *base, size_t elems)
		: base(base), elems(elems) {
			MPI_Win_create(base, elems*sizeof(T), sizeof(T), MPI_INFO_NULL, MPI_COMM_WORLD, &array_window);
		}
		void store_public(T desired, argo::node_id_t rank, size_t idx) {
			sem_wait(&ibsem);
			MPI_Win_lock(MPI_LOCK_EXCLUSIVE, rank, 0, array_window);
			MPI_Put(&desired, 1, mpi_type, rank, idx, 1, mpi_type, array_window);
			MPI_Win_unlock(rank, array_window);
			sem_post(&ibsem);
		}
		void store_local(T desired, size_t idx) {
			argo::node_id_t rank = backend::node_id();
			// sem_wait(&ibsem);
			MPI_Win_lock(MPI_LOCK_EXCLUSIVE, rank, 0, array_window);
			base[idx] = desired;
			MPI_Win_unlock(rank, array_window);
			// sem_post(&ibsem);
		}
		void store(T desired, argo::node_id_t rank, size_t idx) {
			if (rank == backend::node_id())
				store_local(desired, idx);
			else
				store_public(desired, rank, idx);
		}
		T load_public(argo::node_id_t rank, size_t idx) {
			sem_wait(&ibsem);
			MPI_Win_lock(MPI_LOCK_SHARED, rank, 0, array_window);
			T ret;
			MPI_Get(&ret, 1, mpi_type, rank, idx, 1, mpi_type, array_window);
			MPI_Win_unlock(rank, array_window);
			sem_post(&ibsem);
			return ret;
		}
		T load_local(size_t idx) {
			argo::node_id_t rank = backend::node_id();
			// sem_wait(&ibsem);
			MPI_Win_lock(MPI_LOCK_SHARED, rank, 0, array_window);
			T ret = base[idx];
			MPI_Win_unlock(rank, array_window);
			// sem_post(&ibsem);
			return ret;
		}
		T load(argo::node_id_t rank, size_t idx) {
			if (rank == backend::node_id())
				return load_local(idx);
			else
				return load_public(rank, idx);
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

	void undo_log::open_group() {
		if (current_group != nullptr)
			throw std::logic_error("There is already an open group.");
		if (group_range->is_full())
			throw std::logic_error("There are no unused groups."); // TODO: ensure some group is committed
		current_group = new group<location_t>(
			entries,
			entry_range->get_end(), // Start at the next entry
			locks,
			d_lock,
			&d_group[group_range->get_end()] // Usable as there is at least one free group slot
		);
		// PM FENCE
		group_range->inc_end(); // Include newly reset group in group buffer
	}

	void undo_log::close_group() {
		if (current_group == nullptr)
			throw std::logic_error("There is no open group to close.");
		closed_groups.push_back(current_group);
		current_group = nullptr;
	}

	bool undo_log::try_commit_group() {
		if (closed_groups.size() == 0) return false; // No closed group to commit
		// TODO: Check precondition for committing is satisfied
		// (Currently there are none)
		group<location_t> *commit_group = closed_groups.front(); // Get pointer to group to commit
		closed_groups.pop_front(); // Remove the group from the volatile queue
		group_range->inc_start(); // Durable commit of the group
		// Now, free volatile references
		// - Return entry slots (advance range start)
		entry_range->inc_start(commit_group->entry_range.get_use());
		// - Return lock slots (push free-to-reuse indices)
		for (size_t lock_idx = commit_group->lock_list.front_idx();
			!(commit_group->lock_list.is_end(lock_idx));
			lock_idx = commit_group->lock_list.next_idx(lock_idx)
		) {
			lock_free.push_back(lock_idx);
		}
		for (size_t lock_idx = commit_group->unlock_list.front_idx();
			!(commit_group->unlock_list.is_end(lock_idx));
			lock_idx = commit_group->unlock_list.next_idx(lock_idx)
		) {
			lock_free.push_back(lock_idx);
		}
		// - Delete the commit group
		delete commit_group;
		return true;
	}

	void undo_log::commit_group() {
		if (!try_commit_group())
			throw std::logic_error("Failed to commit group.");
		// TODO: requires smarter implementation that waits until commit is possible.
		// printf("Comitted a group.\n");
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
		init_offset += durable_alloc(d_lock, locks, init_offset);
		// printf("d_lock address: %p\n", d_lock);
		init_offset += durable_alloc(d_lock_mailbox, locks, init_offset);
		// printf("d_lock_mailbox address: %p\n", d_lock_mailbox);
		init_offset += durable_alloc(d_group, groups, init_offset);
		// printf("d_group address: %p\n", d_group);
		init_offset += durable_alloc(d_log, 1, init_offset);
		// printf("d_log address: %p\n", d_log);
		entry_range = new range(entries, 0, nullptr);
		for (size_t i = 0; i < locks; ++i)
			lock_free.push_back(i);
		group_range = new range(groups, 0, &d_log->group_range);
		current_group = nullptr; // No open group
		log_lock = new locallock::ticket_lock();
		return init_offset - offset;
	}

	void undo_log::ensure_available_entry() {
		while (entry_range->is_full()) { // While to account for "empty groups"
			// TODO: handle case when all entries are used by single (open) group, i.e. when there is no closed group
			if (closed_groups.size() == 0)
				throw std::logic_error("The open group has used all log entries.");
			commit_group();
		}
	}

	void undo_log::ensure_available_lock() {
		while (lock_free.empty()) { // While to account for "empty groups"
			// TODO: handle case when all locks are used by single (open) group, i.e. when there is no closed group
			if (closed_groups.size() == 0)
				throw std::logic_error("The open group has used all log locks.");
			commit_group();
		}
	}

	void undo_log::ensure_available_group() {
		if (group_range->is_full()) {
			// TODO: handle case when only one group exists
			if (closed_groups.size() == 0)
				throw std::logic_error("There is only a single group and it is open.");
			commit_group();
		}
	}

	void undo_log::ensure_open_group() {
		if (current_group == nullptr) {
			// No open group, open one
			ensure_available_group();
			open_group();
		}
	}

	void undo_log::record_original(location_t location, char *original_data) {
		assert(("The location shouldn't be in the open group.",
			current_group == nullptr || current_group->entry_lookup.count(location) == 0));
		// Close group if exceedign limits (TODO: temporary solution, closing a group will require an APB)
		if (current_group != nullptr && current_group->entry_lookup.size() >= group_size_limit) {
			// group reached size limit, close it
			persistence_arbiter.request_apb();
		}
		// Ensure available resources
		ensure_available_entry();
		ensure_open_group();
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
		current_group->entry_lookup[location] = idx;
		entry_range->inc_end();
	}

	void undo_log::record_changes(location_t location, char *modified_data, char *original_data) {
		std::lock_guard<locallock::ticket_lock> lock(*log_lock);
		size_t idx;
		try {
			if (current_group == nullptr)
				throw std::out_of_range("No open group to in which to record changes.");
			idx = current_group->entry_lookup.at(location);
		} catch (std::out_of_range &e) {
			// Either due to lack of current group or the entry is new.
			record_original(location, original_data);
			idx = current_group->entry_lookup.at(location);
		}
		d_change[idx].update(modified_data, original_data);
	}

	void undo_log::freeze() {
		std::lock_guard<locallock::ticket_lock> lock(*log_lock);
		if (current_group != nullptr)
			close_group();
	}

	void undo_log::commit() {
		std::lock_guard<locallock::ticket_lock> lock(*log_lock);
		if (current_group != nullptr)
			close_group();
		while (!closed_groups.empty())
			commit_group();
	}

}