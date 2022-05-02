#ifndef argo_persistence_hpp
#define argo_persistence_hpp argo_persistence_hpp

#include <atomic>
#include <deque>
#include <mutex>
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

	struct durable_log;

	class undo_log {

		using location_t = argo::memory_t;

		static const size_t entry_size = 4096; // TODO: From swdsm.cpp, should be imported from a header
		static const size_t dirty_unit = 1; // TODO: Should be imported from a header

		static const size_t alignment = entry_size; // TODO: Should be imported from a header

		static const size_t entries = 2048; // TODO: Should be imported from elsewhere or be part of initialisation
		static const size_t groups = 64; // TODO: Should be imported from elsewhere or be part of initialisation
		static const size_t group_size_limit = entries*0.5; // TODO: Should be clear this will be a soft limit, also should be configurable
		// Note: The group size limit should at least leave enough space to hold a full write buffer, perferably a bit more to be safe.

		durable_original<entry_size> *d_original;
		durable_change<entry_size, dirty_unit> *d_change;
		location_t *d_location;

		range *entry_range;

		durable_range *d_group;
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
			// (1/) Wait for other threads to allow.
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
				printf("join apb\n");
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

		/** @brief Registry for trackers belonging to threads.
		 * This register enables tracker creation on a per thread basis,
		 * with a centralised lookup for trackers of individual threads.
		 */
		class registry {

		private:

			/** @brief Pointer to the arbiter to request trackers from. */
			apb_arbiter *arbiter;
			/** @brief The registry mapping threads to trackers. */
			std::unordered_map<pthread_t, tracker*> reg;
			/** @brief Lock to protect the @c reg map from data races. */
			locallock::ticket_lock registry_lock;

		public:

			registry(apb_arbiter *arbiter) : arbiter(arbiter) {}

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
			tracker *get_tracker() {
				std::lock_guard<locallock::ticket_lock> lock(registry_lock);
				return reg[pthread_self()];
			}

			/** @brief Unregisters and deleted the tracker associated with the calling thread. */
			void unregister_thread() {
				std::lock_guard<locallock::ticket_lock> lock(registry_lock);
				pthread_t tid = pthread_self();
				assert(("Thread is not registered.", reg.count(tid) != 0));
				delete reg[tid];
				reg.erase(tid);
			}

		};

	};

}

extern argo::backend::persistence::undo_log persistence_log;
extern argo::backend::persistence::apb_arbiter persistence_arbiter;
extern argo::backend::persistence::apb_arbiter::registry persistence_registry;

namespace argo::backend::persistence {

	/** @brief Wrapper for locks to automatically allow APBs while locking and unlocking. */
	template<typename LockType>
	class persistence_lock {
		LockType &lock_var;
	public:
		persistence_lock(LockType &lock_var) : lock_var(lock_var) {}
		void lock() {
			persistence_registry.get_tracker()->allow_apb();
			lock_var.lock();
			persistence_registry.get_tracker()->prohibit_apb();
		}
		void unlock() {
			persistence_registry.get_tracker()->allow_apb();
			lock_var.unlock();
			persistence_registry.get_tracker()->prohibit_apb();
		}
	};

}

#endif /* argo_persistence_hpp */