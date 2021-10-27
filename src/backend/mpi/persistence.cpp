#include <atomic>
#include <cstdlib>
#include <cstring>
#include <unordered_map>

#include "../../synchronization/intranode/ticket_lock.hpp"
#include "../backend.hpp"

#define ROUND_UP_DIV(n, d) ( ((n)-1)/(d) + 1 )
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
		size_t entry_size = 4096,
		size_t dirty_unit = 1
	>
	class undo_log : public undo_log_base<location_t> {

	private:

		/** @brief A storage link in the undo log. */
		class link {

		private:

			/** @brief Next link in the chain. */
			link *next;

			/** @brief Identifier of the data location monitored by this link. */
			location_t location;

			/** @brief Container of original, unmodified data. Stored persistently.
			 * @todo Ensure persistence.
			 */
			char original_data[entry_size];

			using change_map_t = uint64_t;
			static const size_t cm_flags_per_index = 64;
			static const size_t change_map_length = ROUND_UP_DIV(entry_size, dirty_unit*cm_flags_per_index);

			/** @brief Container of dirty bits. */
			change_map_t change_map[change_map_length] = {0};

		public:

			/** @brief Create a new undo log link.
			 * @param next Next link in the chain.
			 * @param location Identifier of the data location.
			 * @param original_data Pointer to original, unmodified data.
			 */
			link(link *next, location_t location, char *original_data)
			: next(next)
			, location(location) {
				memcpy(this->original_data, original_data, entry_size);
				pm_fence();
			}

			/** @brief Update dirty bits for new changes.
			 * Updates the dirty bits according to the difference
			 * between @p modified_data and the stored original,
			 * alternatively the @p original_data if supplied.
			 * @param modified_data Pointer to the modified version of the data.
			 * @param original_data Optional pointer to unmodified data.
			 *                      Only observed differences to this data
			 *                      instead of the stored original.
			 */
			void update_changes(char *modified_data, char *original_data = nullptr) {
				if (original_data == nullptr)
					original_data = this->original_data;
				size_t data_index = 0;
				FOR_RANGE_COND(size_t, cm_index, 0, change_map_length, data_index < entry_size) {
					change_map_t cm_entry = 0;
					FOR_RANGE_COND(size_t, cm_subindex, 0, cm_flags_per_index, data_index < entry_size) {
						change_map_t cm_subentry = 0;
						FOR_RANGE(size_t, dirty_index, 0, dirty_unit) {
							// Cond (data_index < entry_size) not needed since dirty_unit divides entry size.
							if (original_data[data_index] != modified_data[data_index])
								cm_subentry = 1; // The dirty unit has changed.
							++data_index;
						}
						cm_entry |= cm_subentry << cm_subindex;
					}
					change_map[cm_index] |= cm_entry;
				}
				pm_fence();
			}

			/** @brief Delete a link and its successors.
			 * @param head The first link in the list.
			 */
			static void delete_list(link *head) {
				while (head != nullptr) {
					link *next = head->next;
					delete head;
					head = next;
				}
			}

		};

		/** @brief Links of the log.
		 * A null pointer indicates that the log is committed.
		 */
		link *log_entries_head = nullptr;

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

		undo_log() {
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