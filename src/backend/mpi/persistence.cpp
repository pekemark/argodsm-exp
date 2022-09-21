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

	void lock_repr::lock_initiate(lock_repr_type *lock_field) {
		persistence_log.lock_initiate(reinterpret_cast<argo::memory_t>(lock_field));
	}

	lock_repr::lock_repr_type lock_repr::try_lock_initiate(lock_repr_type *lock_field, lock_repr_type old_field) {
		return persistence_log.try_lock_initiate(reinterpret_cast<argo::memory_t>(lock_field), old_field);
	}

	void lock_repr::lock_success(lock_repr_type *lock_field, lock_repr_type old_field, lock_repr_type new_field) {
		persistence_log.lock_success(reinterpret_cast<argo::memory_t>(lock_field), old_field, new_field);
	}

	void lock_repr::lock_fail(lock_repr_type *lock_field) {
		persistence_log.lock_fail(reinterpret_cast<argo::memory_t>(lock_field));
	}

	lock_repr::lock_repr_type lock_repr::unlock(lock_repr_type *lock_field) {
		return persistence_log.unlock(reinterpret_cast<argo::memory_t>(lock_field));
	}

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
			if (edge.prev == num_nodes) edge.prev = idx; // Set tail if list is empty
			else           ptr.at(edge.next).prev = idx; // Otherwise, set prev of former head
			edge.next = idx; // Update head
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
		size_t committing_group; // group currently being committed. Should be ignored if index is still in group range.
		size_t lock_pending_head; // locks in progress
		size_t lock_unlink; // lock being unlinked from lock_pending. May be duplicated in lock_pending or head of current group. Next pointer not valid.
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
		memset((void*)addr, 0, size);
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
		// Reset limit counters
		group_flushes = 0;
		group_time = std::chrono::steady_clock::now();
	}

	void undo_log::close_group() {
		if (current_group == nullptr)
			throw std::logic_error("There is no open group to close.");
		closed_groups.push_back(current_group);
		current_group = nullptr;
	}

	bool undo_log::try_commit_group() {
		if (closed_groups.size() == 0) { return false; } // No closed group to commit
		group<location_t> *commit_group = closed_groups.front(); // Get pointer to group to commit
		// Check precondition for committing is satisfied
		// - Check that all locks has been persisted
		for (size_t lock_idx = commit_group->lock_list.front_idx();
			!(commit_group->lock_list.is_end(lock_idx));
			lock_idx = commit_group->lock_list.next_idx(lock_idx)
		) {
			lock_repr::lock_repr_type mailbox_field = mpi_mailbox->load_local(lock_idx);
			if (lock_repr::is_awaiting_persist(mailbox_field)) { // Set to awaiting persist when allocated, cleared when persisted
				return false; // Still waiting for some persist, cannot commit
			}
		}
		// Precondition met, start commit
		size_t committing_group = group_range->get_start();
		d_log->committing_group = committing_group;
		// PM FENCE
		// Mark as committed
		closed_groups.pop_front(); // Remove the group from the volatile queue
		group_range->inc_start(); // Durable commit of the group
		// Message locks about commit
		for (size_t lock_idx = commit_group->unlock_list.front_idx();
			!(commit_group->unlock_list.is_end(lock_idx));
			lock_idx = commit_group->unlock_list.next_idx(lock_idx)
		) {
			lock_repr::lock_repr_type expect_field = d_lock[lock_idx].data.new_field; // TODO: Reads from PM, eliminate?
			lock_repr::lock_repr_type persist_field = lock_repr::clear_awaiting_persist(expect_field);
			lock_repr::lock_repr_type mailbox_field = mpi_mailbox->load_local(lock_idx);
			if (lock_repr::is_init(mailbox_field)) {
				// No one has requested a persist notification, try to update global lock field
				argo::data_distribution::global_ptr<lock_repr::lock_repr_type> global_lock_field(reinterpret_cast<lock_repr::lock_repr_type*>(d_lock[lock_idx].data.location)); // TODO: Very ugly, 1) reads from PM, 2) requires cast
				if (backend::atomic::compare_exchange(global_lock_field, expect_field, persist_field, argo::atomic::memory_order::relaxed)) {
					continue;
				} else {
					// Someone else has taken the lock but not yet requested a persist notification
					// Wait for that node to inform this node
					do { mailbox_field = mpi_mailbox->load_local(lock_idx); }
					while (lock_repr::is_init(mailbox_field));
				}
			}
			// Send something other than the init value (for example what we tried to CAS write)
			// to the node and lock offset cound in the local mailbox once updated.
			mpi_mailbox->store_public(
				persist_field,
				lock_repr::get_user(mailbox_field),
				lock_repr::get_lock_offset(mailbox_field)
			);
		}
		// Commit complete, clear committing group
		d_log->committing_group = groups; // out-of-range - "null" value
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
		// Report success
		return true;
	}

	void undo_log::commit_group() {
		while (!try_commit_group()) {} // TODO: This could cause issues as the log lock is being blocked. If temporarily unlocking, caller should be aware.
			// throw std::logic_error("Failed to commit group.");
		// TODO: requires smarter implementation that waits until commit is possible.
		// printf("Comitted a group.\n");
	}

	size_t undo_log::flush_groups() {
		size_t count = 0;
		while (try_commit_group()) {
			count += 1;
		}
		if (current_group != nullptr) {
			++group_flushes;
			// TODO: Can be skipped if an APB is already requested.
			if (group_flushes >= group_flush_limit) {
				persistence_arbiter.request_apb(); // TODO: Should be requested a better way. The log shouldn't depend on the arbiter.
			} else if (std::chrono::steady_clock::now() - group_time >= group_time_limit) {
				// TODO: The time-based request may fit better elsewhere (as long as the log lock is taken when checking for current group existance).
				persistence_arbiter.request_apb(); // TODO: Should be requested a better way. The log shouldn't depend on the arbiter.
			} else {
				// TODO: The unlock-based request may fit better elsewhere (as long as the log lock is taken when checking for current group existance and walking the unlock list).
				// TODO: This code requests an apb if one of the unlocked locks
				//       in the requested group has been taken. However, it is
				//       bad that the code goes through the unlock list all the time.
				//       Once an apb is ordered this can stop.
				//       Also, it may be worth only doing this periodically
				//       (i.e. every nth flush where n is configurable).
				for (size_t lock_idx = current_group->unlock_list.front_idx();
					!(current_group->unlock_list.is_end(lock_idx));
					lock_idx = current_group->unlock_list.next_idx(lock_idx)
				) {
					lock_repr::lock_repr_type mailbox_field = mpi_mailbox->load_local(lock_idx);
					if (!lock_repr::is_init(mailbox_field)) {
						persistence_arbiter.request_apb();  // TODO: Should be requested a better way. The log shouldn't depend on the arbiter.
						break;
					}
				}
			}
		}
		return count;
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
		mpi_mailbox = new mpi_atomic_array<lock_repr::lock_repr_type>(d_lock_mailbox, locks);
		for (size_t i = 0; i < locks; ++i)
			lock_free.push_back(i);
		pending_locks = new list<location_t, durable_lock<location_t>>(&d_log->lock_pending_head, d_lock, locks);
		d_log->lock_unlink = locks; // out-of-range - "null" value
		d_log->committing_group = groups; // out-of-range - "null" value
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
		assert(((void)"The location shouldn't be in the open group.",
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
		assert(((void)"The end of the global entry range should match that of the current group.",
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
		flush_groups();
	}

	void undo_log::commit() {
		std::lock_guard<locallock::ticket_lock> lock(*log_lock);
		if (current_group != nullptr)
			close_group();
		while (!closed_groups.empty())
			commit_group();
	}

	size_t undo_log::flush() {
		std::lock_guard<locallock::ticket_lock> lock(*log_lock);
		return flush_groups();
	}

	size_t undo_log::allocate_lock_node() {
		// Ensure available resources
		ensure_available_lock();
		// Allocate a lock node to use
		size_t lock_node = lock_free.front();
		lock_free.pop_front();
		return lock_node;
	}

	void undo_log::deallocate_lock_node(size_t lock_node) {
		lock_free.push_back(lock_node);
	}

	void undo_log::init_lock_node(
		size_t lock_node,
		location_t addr,
		lock_repr::lock_repr_type old_data,
		lock_repr::lock_repr_type new_data
	) {
		d_lock[lock_node].data.old_field = old_data;
		d_lock[lock_node].data.new_field = new_data;
		d_lock[lock_node].data.location = addr;
		mpi_mailbox->store_local(lock_repr::set_awaiting_persist(lock_repr::make_init()), lock_node);
	}

	template<typename Key, typename T>
	void undo_log::link_lock_node(
		size_t lock_node,
		Key key,
		list<Key, T> *lock_list
	) {
		lock_list->push_front(key, lock_node);
	}

	void undo_log::init_and_link_pending_lock_node(
		size_t lock_node,
		location_t addr,
		lock_repr::lock_repr_type old_data,
		lock_repr::lock_repr_type new_data
	) {
		// Initialise durable lock node
		init_lock_node(lock_node, addr, old_data, new_data);
		// Link lock node to pending locks
		link_lock_node(lock_node, addr, pending_locks);
	}

	void undo_log::lock_initiate(location_t addr) {
		std::lock_guard<locallock::ticket_lock> lock(*log_lock);
		size_t lock_node = allocate_lock_node();
		// Register lock node to be reused after failures
		assert(((void)"There is already an allocated lock for the address.", retry_locks.count(addr)==0));
		retry_locks[addr] = lock_node;
		// Initialise and link durable lock node
		init_and_link_pending_lock_node(lock_node, addr, lock_repr::make_init(), lock_repr::make_init());
		// Old and new are the same, so no change if new should match on recovery.
	}

	lock_repr::lock_repr_type undo_log::try_lock_initiate(
		location_t addr,
		lock_repr::lock_repr_type old_data
	) {
		std::unique_lock<locallock::ticket_lock> lock(*log_lock);
		size_t lock_node;
		try {
			// Get already allocated lock
			lock_node = retry_locks.at(addr);
		} catch (std::out_of_range &e) {
			lock_node = locks; // out-of-range - "null" value
		}
		lock_repr::lock_repr_type new_data;
		if (lock_node < locks) { // Lock node found
			// Construct new lock field
			new_data = lock_repr::make_field(true, true, backend::node_id(), lock_node);
			// Update lock node
			d_lock[lock_node].data.new_field = new_data; // update new first as it shouldn't be possible for any other node to write this value.
			// PM FENCE
			d_lock[lock_node].data.old_field = old_data;
			// PM FENCE
			assert(((void)"The address of the already allocatied lock node doesn't match.", d_lock[lock_node].data.location == addr));
		} else { // No lock registered, this is a try-lock
			lock_node = allocate_lock_node();
			// Construct new lock field
			new_data = lock_repr::make_field(true, true, backend::node_id(), lock_node);
			// Persistently update lock data
			init_and_link_pending_lock_node(lock_node, addr, old_data, new_data);
		}
		// Avoid dependence cycles
		if (current_group != nullptr // No cycle possible if lock is placed in a new group
			&& !current_group->unlock_list.empty() // No cycle possible if no locks has been released
			// TODO: check that some lock has been acquired by another node (neet to double chek this makes sense) // No cycle possible if no released locks has been acquired
		) {
			// The current group already has already released a lock, the incomming acquire might constitute a cycle (if comming from a different node). Force a new group (trhrough APB).
			// TODO: This could be skipped if the lock doesn't come from a different node (then there is no risk of a cycle)
			//       if the lock comes from an earlier group, there is no problem
			//       if the lock comes from the same group, but then the lock list must support multiple entries for the same address.
			lock.unlock();
			persistence_registry.get_tracker()->make_apb(); // TODO: forcing an apb may be considered an error, yet the apb is needed under certain circumstances
			// lock.lock(); // retrurning next, no need to relock
		}
		return new_data;
	}

	void undo_log::lock_success(
		location_t addr,
		lock_repr::lock_repr_type old_data,
		lock_repr::lock_repr_type new_data
	) {
		// Request persist notification
		// - If old_data indicates persisted, copy to mailbox
		if (!lock_repr::is_awaiting_persist(old_data)) {
			mpi_mailbox->store_local(old_data, lock_repr::get_lock_offset(new_data));
		}
		// - Else, write new_data to the mailbox indicated in old_data
		else {
			argo::node_id_t prev_node = lock_repr::get_user(old_data);
			size_t prev_node_lock = lock_repr::get_lock_offset(old_data);
			mpi_mailbox->store_public(new_data, prev_node, prev_node_lock);
		}
		// Start log updates
		std::unique_lock<locallock::ticket_lock> lock(*log_lock);
		// Ensure available resources
		ensure_open_group();
		// Attach the successful lock to the group's lock list.
		// - Find lock associated with addr
		size_t unlink_idx = pending_locks->lookup_idx(addr);
		// - Persistently keep track of the node being unlinked
		d_log->lock_unlink = unlink_idx;
		// PM FENCE
		// - Point unlinked's prev's next to unlinked's next
		pending_locks->unlink_idx(unlink_idx);
		// - Point unlinked's next to group's head
		// - Point group's head to unlinked
		current_group->lock_list.push_front(addr, unlink_idx);
		// - Clear unlink
		d_log->lock_unlink = locks;
		// PM FENCE
		// Add lock addr & new_field to held_locks
		held_locks.insert({addr, d_lock[unlink_idx].data.new_field}); // TODO: sub-optimal, shouldn't get new field from PM
		// Remove from retry locks
		retry_locks.erase(addr);
	}

	void undo_log::lock_fail(location_t addr) {
		std::lock_guard<locallock::ticket_lock> lock(*log_lock);
		if (retry_locks.count(addr) == 0) {
			// This was a try-lock and the program may do something else after the failure, remove the lock from pending locks.
			size_t lock_node = pending_locks->lookup_idx(addr);
			pending_locks->unlink_idx(lock_node);
			deallocate_lock_node(lock_node);
		}
	}

	lock_repr::lock_repr_type undo_log::unlock(location_t addr) {
		std::lock_guard<locallock::ticket_lock> lock(*log_lock);
		// Get the old field (that should still be in global mem)
		lock_repr::lock_repr_type old_field;
		try {
			old_field = held_locks.at(addr);
		} catch (std::out_of_range &e) {
			throw std::logic_error("The lock being unlocked is currently not being held.");
		}
		// Allocate a slot in the log
		size_t lock_node = allocate_lock_node();
		// Create the new field (unlocked but awaiting persist, signed by node and lock slot)
		lock_repr::lock_repr_type new_field = lock_repr::make_field(false, true, backend::node_id(), lock_node);
		// Initialise and link durable lock node
		init_lock_node(lock_node, addr, old_field, new_field);
		link_lock_node(lock_node, addr, &current_group->unlock_list);
		// Remove from held_locks
		held_locks.erase(addr);
		return new_field;
	}

}