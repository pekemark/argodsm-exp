#ifndef argo_persistence_hpp
#define argo_persistence_hpp argo_persistence_hpp

#include <atomic>
#include <deque>
#include <mutex>
#include <type_traits>
#include <unordered_map>

#include "../../synchronization/intranode/ticket_lock.hpp"
#include "types/types.hpp"

namespace argo::backend::persistence {
	class undo_log;
	class apb_arbiter;
	class thread_registry;
}

extern argo::backend::persistence::undo_log persistence_log;
extern argo::backend::persistence::apb_arbiter persistence_arbiter;
extern argo::backend::persistence::thread_registry persistence_registry;

namespace argo::backend::persistence {

	class lock_repr {
	public:
		/** @brief lock representation type this class supports
		 * (msb) lpxxxxxi LLL GGG UUUU UUUU (lsb) (64 bit)
		 * Lower case = 1 bit, upper case = 4 bit
		 * l = locked
		 * p = awaiting persist
		 * i = initial state (other data invalid)
		 * L = Lock offset
		 * G = Group offset
		 * U = User (Node ID)
		 * x = reserved
		 */
		using lock_repr_type = std::uint64_t;
	private:
		/** @brief constant signifying lock is in an initial state and free */
		static const lock_repr_type init_bit = 1UL<<56;
		/** @brief constant signifying lock is taken */
		static const lock_repr_type locked_bit = 1UL<<63;
		static const lock_repr_type awaiting_persist_bit = 1UL<<62;

		static const std::size_t user_size = 32;
		static const std::size_t user_shift = 0;
		static const lock_repr_type user_mask = (1UL<<user_size) - 1;

		static const std::size_t group_size = 12;
		static const std::size_t group_shift = user_shift + user_size;
		static const lock_repr_type group_mask = (1UL<<group_size) - 1;

		static const std::size_t lock_size = 12;
		static const std::size_t lock_shift = group_shift + group_size;
		static const lock_repr_type lock_mask = (1UL<<lock_size) - 1;


		/** @brief internal alias for determining node ID */
		static argo::node_id_t inline self() { return backend::node_id(); }

	public:
		// Methods for information extraction. Side-effect free.

		/** @brief Determines whether the lock field represents the an initial state.
		 * @param lock_field The lock field to inspect.
		 * @return True if the lock field is in an initial state, otherwise false.
		 */
		static bool inline is_init(lock_repr_type lock_field) { return (lock_field & init_bit) != 0; }

		/** @brief Determines whether the lock field represents the a locked state.
		 * @param lock_field The lock field to inspect.
		 * @return True if the lock field is in a locked state, otherwise false.
		 */
		static bool inline is_locked(lock_repr_type lock_field) { return (lock_field & locked_bit) != 0; }

		/** @brief Extracts the user information in the lock_field.
		 * Undefined for a lock field in an initial state.
		 * @param lock_field The lock field to inspect.
		 * @return The node ID of the user encoded in the lock field.
		 */
		static argo::node_id_t inline get_user(lock_repr_type lock_field) {
			return static_cast<argo::node_id_t>((lock_field >> user_shift) & user_mask);
		}

		/** @brief Determines whether the lock field encodes the specified user.
		 * Undefined for a lock field in an initial state.
		 * @param lock_field The lock field to inspect.
		 * @param node Node ID to check against.
		 * @return True if the lock field encodes the specified user.
		 */
		static bool inline is_user(lock_repr_type lock_field, argo::node_id_t node) { return get_user(lock_field) == node; }

		/** @brief Determines whether the lock field encodes the local node as the user.
		 * Undefined for a lock field in an initial state.
		 * @param lock_field The lock field to inspect.
		 * @return True if the lock field encodes the local node as the user.
		 */
		static bool inline is_user_self(lock_repr_type lock_field) { return is_user(lock_field, self()); }


		// Methods for creating lock fields with a specific state. Side-effect free

		static lock_repr_type inline make_field() { return init_bit; }
		static lock_repr_type inline make_field(bool locked, bool awaiting_persist, argo::node_id_t user, std::size_t group_idx, std::size_t lock_idx) {
			lock_repr_type field = 0;
			if (locked) field |= locked_bit;
			if (awaiting_persist) field |= awaiting_persist_bit;
			field |= (user & user_mask) << user_shift;
			field |= (group_idx & group_mask) << group_shift;
			field |= (lock_idx & lock_mask) << lock_shift;
			return field;
		}

		/** @brief Creates a lock field in a locked state.
		 * @param node Node ID to encode as the user of the lock.
		 * @return A lock field in an unlocked state, encoded with the specified user.
		 */
		static lock_repr_type inline make_locked(argo::node_id_t node) { return make_field(true, true, node, 0, 0); }

		/** @brief Creates a lock field in a locked state.
		 * @return A lock field in a locked state, encoded with the local node as the user.
		 */
		static lock_repr_type inline make_locked() { return make_locked(self()); }

		/** @brief Creates a lock field in an unlocked state.
		 * @param node Node ID to encode as the user of the lock.
		 * @return A lock field in an unlocked state, encoded with the specified user.
		 */
		static lock_repr_type inline make_unlocked(argo::node_id_t node) { return make_field(false, true, node, 0, 0); }

		/** @brief Creates a lock field in an unlocked state.
		 * @return A lock field in an unlocked state, encoded with the local node as the user.
		 */
		static lock_repr_type inline make_unlocked() { return make_unlocked(self()); }

		/** @brief Creates a lock field in an initial state.
		 * @return A lock field in an initial state.
		 */
		static lock_repr_type inline make_init() { return init_bit; }


		// Methods for registering important lock events. May have side-effects.

		/** @brief Registers intention to lock on the specified lock field with retries until successful.
		 * @param lock_field Pointer to the lock field to lock on.
		 */
		static void inline lock_initiate(lock_repr_type *lock_field) {}

		/** @brief Registers intention to lock on the specified lock field. This intent is allowed to fail.
		 * @param lock_field Pointer to the lock field to lock on.
		 * @param old_field The expected value of the lock field.
		 * @return A locked lock field appropriate to become the new value of the targeted lock field.
		 */
		static lock_repr_type inline try_lock_initiate(lock_repr_type *lock_field, lock_repr_type old_field) {
			return make_locked();
		}

		/** @brief Registers a successfull lock aquisition on the specified lock field.
		 * This call should follow a call to @c try_lock_initiate with the same lock field.
		 * @param lock_field Pointer to the lock field that has been locked.
		 */
		static void inline lock_success(lock_repr_type *lock_field) {}

		/** @brief Registers a failed lock aquisition on the specified lock field.
		 * This call should follow a call to @c try_lock_initiate with the same lock field.
		 * @param lock_field Pointer to the lock field that could not be locked.
		 */
		static void inline lock_fail(lock_repr_type *lock_field) {}

		/** @brief Registers intent to unlock the specified lock field.
		 * @return An unlocked lock field appropriate to become the new value of the targeted lock field.
		 */
		static lock_repr_type inline unlock(lock_repr_type *lock_field) {
			return make_unlocked();
		}
	};

	template<size_t entry_size>
	struct durable_original;

	template<size_t entry_size, size_t dirty_unit>
	struct durable_change;

	struct durable_range;

	class range;

	template<typename location_t>
	struct durable_lock;

	struct durable_group;

	template<typename location_t>
	struct group;

	struct durable_log;

	class undo_log {

		using location_t = argo::memory_t;

		static const size_t entry_size = 4096; // TODO: From swdsm.cpp, should be imported from a header
		static const size_t dirty_unit = 1; // TODO: Should be imported from a header

		static const size_t alignment = entry_size; // TODO: Should be imported from a header

		static const size_t entries = 2048; // TODO: Should be imported from elsewhere or be part of initialisation
		static const size_t locks = 2048; // TODO: Should be imported from elsewhere or be part of initialisation
		static const size_t groups = 64; // TODO: Should be imported from elsewhere or be part of initialisation
		static const size_t group_size_limit = entries*0.5; // TODO: Should be clear this will be a soft limit, also should be configurable
		// Note: The group size limit should at least leave enough space to hold a full write buffer, perferably a bit more to be safe.

		durable_original<entry_size> *d_original;
		durable_change<entry_size, dirty_unit> *d_change;
		location_t *d_location;
		range *entry_range;

		durable_lock<location_t> *d_lock;
		lock_repr::lock_repr_type *d_lock_mailbox; // TODO: Does this need to be persistent?
		range *lock_range;

		durable_group *d_group;
		range *group_range;
		std::deque<group<location_t>*> closed_groups;
		group<location_t> *current_group;

		durable_log *d_log;

		/** @brief Handling exclusive access for the structure. */
		locallock::ticket_lock *log_lock;

		template<typename T>
		static size_t durable_alloc(T *&addr, size_t copies, size_t offset);

		void open_group();
		void close_group();
		bool try_commit_group();
		void commit_group();

		void ensure_available_entry();
		void ensure_available_lock();
		void ensure_available_group();
		void ensure_open_group();

		void record_original(location_t location, char *original_data);

	public:

		size_t initialize(size_t offset);

		void record_changes(location_t location, char *modified_data, char *original_data);

		void freeze(); // Closes the open group
		void commit(); // Closes the open group and blocks until all groups are committed

	};

	/** @brief Arbiter for Aligned Persist Barrier.
	 * A persist barrier is performed by closing a group in the underlying
	 * persistence log. This class coordinates participating actors to create
	 * opportuinities to perform aligned persist barriers (APBs).
	 * To function properly, all actors that use the
	 * persistence log attached to an arbiter has to get a tracker from said
	 * arbiter and indicate when it is and is not ready for an APB.
	 * Actors should take care not to stall without allowing APBs and
	 * must eventually (prefereably with sufficient frequency) allow APBs.
	 */
	class apb_arbiter {

	private:

		/** @brief Indicates that an APB is requested. */
		std::atomic_bool apb_requested;
		/** @brief Indicates that an APB is underway. */
		std::atomic_bool apb_in_progress;
		/** @brief Count of trackers that prevents an APB. */
		std::atomic_size_t prohibiting_trackers;
		/** @brief Pointer to attached persistence log. */
		undo_log *log;

	public:

		/** @copydoc apb_arbiter
		 * @param log The persistence log to attach to the arbiter.
		 */
		apb_arbiter(undo_log *log)
		: apb_requested(false)
		, apb_in_progress(false)
		, prohibiting_trackers(0)
		, log(log) {}

		~apb_arbiter() {}

		/** @brief Stalls until an APB has completed.
		 * @note A new APB could start right after the return.
		 */
		void wait_apb() {
			while (apb_in_progress) {}
		}

		/** @brief Requests an APB to be done as soon as possible.
		 * This method is a soft form of APB that isn't perfomed immediately.
		 * Instead, the APB will be performed by otherthreads as they join
		 * with an APB.
		 */
		void request_apb() {
			apb_requested.store(true);
		}

	private:

		/** @brief Performs an APB if one has been requested.
		 * Returns immediatly if no apb has been requested or one is already
		 * underway.
		 * @warning May not be called by an actor prohibiting APBs,
		 *          this could result in a deadlock.
		 */
		void handle_apb_request() {
			if (apb_requested) {
				try_make_apb(); // Includes a release which may cause additional APB request. This APB will serve, however.
				/* Note: This is a point of contention. There must be enough
				 * space in the log to complete a release (which may add
				 * additional entries to the log) in order to finally freeze
				 * a group (which is the goal of the APB) and thereby
				 * completing the first step toward commiting the group,
				 * which will free up space in the log.
				 */
			}
		}

		/** @brief Remove a tracker as prohibiting.
		 * This method should be used when an actor may stall for a longer
		 * period while an APB is acceptable.
		 */
		void allow_apb() {
			prohibiting_trackers--;
		}

		/** @brief Add a tracker as prohibiting.
		 * This method should be used when when it is no longer acceptable for
		 * an actor to experience an APB. The method will
		 * stall until no APB is in progress.
		 */
		void prohibit_apb() {
			// Wait for any APB to complete.
			wait_apb();
			// A new APB could be initiated here...
			prohibiting_trackers++;
			// ...if so it should be joined.
			join_apb(); // Mutual tail-recurson, compiler's tail-call optimisation should deal with it.
			// Why join a new APB?
			// If not, the APB could interfere with the following SFR.
			// If just waiting for the APB to complete, a deadlock could occur.
		}

		/** @brief Checks if any APB is in progress and allows it to proceed.
		 * If actors doesn't periodically allow APBs, this method
		 * should be called when it is acceptable to have an APB.
		 */
		void join_apb() {
			if (apb_in_progress) {
				allow_apb();
				prohibit_apb();
			}
		}

	public:

		/** @brief Starts an APB if one is not underway.
		 * @return @c false if an APB was already in progres,
		 *         otherwise @c true once the APB has completed.
		 * @warning May not be called by an actor prohibiting APBs,
		 *          this would result in a deadlock.
		 */
		bool try_make_apb() {
			bool expected = false;
			if (!apb_in_progress.compare_exchange_strong(expected, true))
				return false; // Did not make an APB, one was in progress.
			// An APB was not in progress, but now it has been started.
			// (1/3) Wait for other threads to allow.
			while (prohibiting_trackers > 0) {}
			// (2/3) Perform APB (i.e., flush write buffer and freeze log group).
			argo::backend::release();
			log->freeze();
			apb_requested = false; // Clear any APB request (after log freeze but before APB is over)
			// (3/3) Allow other threads to continue.
			apb_in_progress = false;
			return true; // Made a restore.
		}

		/** @brief Performs an APB.
		 * This method performs an APB, alternatively waits for the
		 * one underway to complete.
		 */
		void make_apb() {
			if (!try_make_apb())
				wait_apb();
		}

		/** @brief Tracker used to interact with an @c apb_arbiter.
		 * This class represents an actor or thread that wants to interact with
		 * the arbiter (and thus uses the arbiter's persistence log).
		 */
		class tracker {

		private:

			/** @brief The @c restore arbiter attached to the @c tracker. */
			apb_arbiter *arbiter;
			/** @brief Indicates whether the tracker is prohibiting APBs. */
			bool prohibiting = false;

		protected:

			friend class apb_arbiter;

			/** @copybrief tracker
			 * @param arbiter The arbiter the tracker should be attached to.
			 * @param prohibit Whether to prohibit APBs immediately.
			 */
			tracker(apb_arbiter *arbiter, bool prohibit = true)
			: arbiter(arbiter) {
				if (prohibit) {
					prohibit_apb();
				}
			}

		public:

			~tracker() { allow_apb(); }

			/** @brief Wether the tracker is prohibiting.
			 * @return Wether the tracker is prohibiting.
			 */
			bool is_prohibiting() { return prohibiting; }

			/** @brief Removes the tracker as prohibiting with the arbiter. */
			void allow_apb() {
				if (prohibiting)
					arbiter->allow_apb();
				prohibiting = false;
				arbiter->handle_apb_request();
			}

			/** @brief Adds the tracker as prohibiting with the arbiter.
			 * @note Will stall until no restore point creation is in progress.
			 */
			void prohibit_apb() {
				if (!prohibiting)
					arbiter->handle_apb_request();
					arbiter->prohibit_apb();
				prohibiting = true;
			}

			/** @brief Join any APB underway with the arbiter.
			 * @note Will stall until no APB is in progress.
			 */
			void join_apb() {
				if (prohibiting)
					if (arbiter->apb_requested) {
						arbiter->allow_apb();
						arbiter->handle_apb_request();
						arbiter->prohibit_apb();
					} else {
						arbiter->join_apb();
					}
				else {
					arbiter->handle_apb_request();
				}
				// The tracker will remain prohibiting afterwards.
			}

			/** @brief Starts or joins an APB with the arbiter.
			 * @note Will stall until the APB is done.
			 * @note Will automatically allow an APB before starting it and
			 *       disallow after if that was the original state.
			 */
			void make_apb() {
				bool was_prohibiting = prohibiting;
				allow_apb();
				arbiter->make_apb();
				if (was_prohibiting)
					prohibit_apb();
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

	/** @brief Registry for trackers belonging to threads.
	 * This register enables tracker creation on a per thread basis,
	 * with a centralised lookup for trackers of individual threads.
	 */
	class thread_registry {

	private:

		/** @brief Pointer to the arbiter to request trackers from. */
		apb_arbiter *arbiter;
		/** @brief The registry mapping threads to trackers. */
		std::unordered_map<pthread_t, apb_arbiter::tracker*> reg;
		/** @brief Lock to protect the @c reg map from data races. */
		locallock::ticket_lock registry_lock;

	public:

		thread_registry(apb_arbiter *arbiter) : arbiter(arbiter) {}

		/** @brief Registers the calling thread with the registry. */
		void register_thread() {
			std::lock_guard<locallock::ticket_lock> lock(registry_lock);
			pthread_t tid = pthread_self();
			assert(("Thread is already registered.", reg.count(tid) == 0));
			reg[tid] = arbiter->create_tracker();
		}

		/** @brief Reteurns the tracker associated with the calling thread.
		 * @return Pointer to the tracker associated with the calling thread.
		 */
		apb_arbiter::tracker *get_tracker() {
			std::lock_guard<locallock::ticket_lock> lock(registry_lock);
			pthread_t tid = pthread_self();
			assert(("Thread is not registered.", reg.count(tid) != 0));
			return reg.at(tid);
		}

		/** @brief Unregisters and deleted the tracker associated with the calling thread. */
		void unregister_thread() {
			std::lock_guard<locallock::ticket_lock> lock(registry_lock);
			pthread_t tid = pthread_self();
			assert(("Thread is not registered.", reg.count(tid) != 0));
			delete reg.at(tid);
			reg.erase(tid);
		}

	};

	/** @brief Wrapper for locks to automatically allow APBs while locking and unlocking. */
	template<typename LockType>
	class persistence_lock {
		LockType *lock_var;
	public:
		persistence_lock(LockType *lock_var) : lock_var(lock_var) {}
		void lock() {
			persistence_registry.get_tracker()->allow_apb();
			lock_var->lock();
			persistence_registry.get_tracker()->prohibit_apb();
		}
		bool try_lock() {
			persistence_registry.get_tracker()->allow_apb();
			bool success = lock_var->try_lock();
			persistence_registry.get_tracker()->prohibit_apb();
			return success;
		}
		void unlock() {
			persistence_registry.get_tracker()->allow_apb();
			lock_var->unlock();
			persistence_registry.get_tracker()->prohibit_apb();
		}
		LockType *get_lock() {
			return lock_var;
		}
	};

	enum persistance_barrier_type {
		/** @brief Use to avoid executing an APB while waiting for the barrier.
		 * Suitable for any barrier. */
		NO_PERSIST_BARRIER,
		/** @brief Use to allow APBs while waiting, but do not enforce one.
		 * Suitable for any barrier. */
		ALLOW_BARRIER,
		/** @brief Use to enforece an APB in accociation to the barrier.
		 * Suitable for (software) node-wide barriers. */
		APB_BARRIER,
		/** @brief Use to commit all outstanding persistent writes
		 * in association with the barrier.
		 * Suitable for system-wide (ArgoDSM) barriers. */
		COMMIT_BARRIER
	};

	template<persistance_barrier_type btype>
	class persistence_barrier_guard {
		apb_arbiter::tracker *ptrack;
		bool prohibiting = false;
	public:
		persistence_barrier_guard() {
			if (btype == NO_PERSIST_BARRIER)
				return; // Doesn't matter whether prohibiting or not.
			ptrack = persistence_registry.get_tracker();
			prohibiting = ptrack->is_prohibiting();
			if (prohibiting)
				ptrack->allow_apb();
			if (btype == APB_BARRIER || btype == COMMIT_BARRIER)
				ptrack->make_apb();
			if (btype == COMMIT_BARRIER)
				persistence_log.commit();
		}
		~persistence_barrier_guard() {
			if (prohibiting)
				ptrack->prohibit_apb();
		}
	};

	/** @brief Wrapper for barriers to automatically perform an APB.
	 * This function will call the supplied barrier function, @p bar ,
	 * with the arguments in @p bargs after optionally allowing APBs,
	 * calling an APB, and commit the entire log, depending on the
	 * requested barrier type, @p btype .
	 * @tparam btype Type of persist action to perform.
	 * @param bar Barrier function to call.
	 * @param bargs Arguemts to supply the barrier function with.
	 * @return The return value of the barrier function.
	*/
	template<persistance_barrier_type btype, typename BarRet, typename... BarArgs>
	BarRet persistence_barrier(BarRet (*bar)(BarArgs...), BarArgs... bargs) {
		persistence_barrier_guard<btype> bar_guard;
		if (std::is_void<BarRet>::value)
			bar(bargs...);
		else
			return bar(bargs...);
	}
	template<typename BarRet, typename... BarArgs>
	BarRet persistence_barrier(persistance_barrier_type btype, BarRet (*bar)(BarArgs...), BarArgs... bargs) {
		switch (btype) {
		case NO_PERSIST_BARRIER:
			return persistence_barrier<NO_PERSIST_BARRIER>(bar, bargs...);
		case ALLOW_BARRIER:
			return persistence_barrier<ALLOW_BARRIER>(bar, bargs...);
		case APB_BARRIER:
			return persistence_barrier<APB_BARRIER>(bar, bargs...);
		case COMMIT_BARRIER:
			return persistence_barrier<COMMIT_BARRIER>(bar, bargs...);
		default:
			throw std::invalid_argument();
		}
	}
	template<typename BarRet, typename... BarArgs>
	BarRet allow_barrier(BarRet (*bar)(BarArgs...), BarArgs... bargs) {
		return persistence_barrier<ALLOW_BARRIER>(bar, bargs...);
	}
	template<typename BarRet, typename... BarArgs>
	BarRet apb_barrier(BarRet (*bar)(BarArgs...), BarArgs... bargs) {
		return persistence_barrier<APB_BARRIER>(bar, bargs...);
	}
	template<typename BarRet, typename... BarArgs>
	BarRet commit_barrier(BarRet (*bar)(BarArgs...), BarArgs... bargs) {
		return persistence_barrier<COMMIT_BARRIER>(bar, bargs...);
	}

}

#endif /* argo_persistence_hpp */