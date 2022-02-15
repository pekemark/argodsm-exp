/**
 * @file
 * @brief This file provides a tas lock for the ArgoDSM system based on the TAS lock made by David Klaftenegger
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#ifndef argo_global_tas_lock_hpp
#define argo_global_tas_lock_hpp argo_global_lock_hpp

#include "../backend/backend.hpp"
#include "../data_distribution/global_ptr.hpp"
#include "../data_distribution/data_distribution.hpp"
#include "../types/types.hpp"
#include <chrono>
#include <thread>
#include <limits>

#include "../backend/mpi/persistence.hpp" // breaks if using single-node

// #define PA_PACKED_COMPACT

namespace argo {
	namespace globallock {
		/** @brief a global test-and-set lock */
		class global_tas_lock {
			private:
				// /** @brief constant signifying that the lock has no last user (all 1:s except for the MSB) */
				// #ifdef PA_PACKED_COMPACT
				// static const uint8_t init = std::numeric_limits<uint8_t>::max() >> 1;
				// #else
				// static const std::size_t init = std::numeric_limits<std::size_t>::max() >> 1;
				// #endif
				// /** @brief mask to retreive the id of the last user (all 1:s except for the MSB) */
				// #ifdef PA_PACKED_COMPACT
				// static const uint8_t id_mask = std::numeric_limits<uint8_t>::max() >> 1;
				// #else
				// static const std::size_t id_mask = std::numeric_limits<std::size_t>::max() >> 1;
				// #endif
				// /** @brief constant signifying lock is taken (1 in the MSB position) */
				// #ifdef PA_PACKED_COMPACT
				// static const uint8_t locked = ~(std::numeric_limits<uint8_t>::max() >> 1);
				// #else
				// static const std::size_t locked = ~(std::numeric_limits<std::size_t>::max() >> 1);
				// #endif
				// /** @brief mask to retreive the lock status (1 in the MSB position) */
				// #ifdef PA_PACKED_COMPACT
				// static const uint8_t locked_mask = ~(std::numeric_limits<uint8_t>::max() >> 1);
				// #else
				// static const std::size_t locked_mask = ~(std::numeric_limits<std::size_t>::max() >> 1);
				// #endif

				// /** @brief import global_ptr */
				// #ifdef PA_PACKED_COMPACT
				// using global_uint8_t = typename argo::data_distribution::global_ptr<uint8_t>;
				// #else
				// using global_size_t = typename argo::data_distribution::global_ptr<std::size_t>;
				// #endif

			public:
				/**
				 * @brief internally used type for lock field
				 * @note this type may change without warning,
				 *       user code must use this type alias
				 */
				using internal_field_type = argo::backend::persistence::lock_data;

				static_assert(sizeof(internal_field_type) <= 8,
					"The internal lock field cannot exceed 8 bytes "
					"due to MPI restrictions for atomics.");

			private:
				/**
				 * @brief global_ptr for the internal_filed_type
				 */
				using global_lock_type = typename argo::data_distribution::global_ptr<internal_field_type>;

				/**
				 * @brief pointer to lock field
				 * @todo should be replaced with an ArgoDSM-specific atomic type
				 *       to allow efficient synchronization over more backends
				 */
				// #ifdef PA_PACKED_COMPACT
				// global_uint8_t lock_data;
				// #else
				// global_size_t lock_data;
				// #endif
				global_lock_type lock_data;

				// #ifdef PA_PACKED_COMPACT
				// static bool inline is_locked(uint8_t data) {
				// #else
				// static bool inline is_locked(std::size_t data) {
				// #endif
				static bool inline is_locked(const internal_field_type &data) {
					return data.locked;
				}

				// #ifdef PA_PACKED_COMPACT
				// static std::size_t inline last_user(uint8_t data) {
				// #else
				// static std::size_t inline last_user(std::size_t data) {
				// #endif
				static argo::node_id_t inline last_user(const internal_field_type &data) {
					return data.last_user;
				}

				// #ifdef PA_PACKED_COMPACT
				// static uint8_t inline get_data(bool lock, std::size_t user) {
				// #else
				// static std::size_t inline get_data(bool lock, std::size_t user) {
				// #endif
				static internal_field_type inline get_data(bool lock, std::size_t user) {
					return internal_field_type(lock, user, 0);
				}

				argo::memory_t inline get_location() {
					return reinterpret_cast<argo::memory_t>(lock_data.get());
				}

			public:
				/**
				 * @brief construct global tas lock from existing memory in global address space
				 * @param f pointer to global field for storing lock state
				 */
				// #ifdef PA_PACKED_COMPACT
				// global_tas_lock(uint8_t* f) : lock_data(global_uint8_t(f)) {
				// #else
				// global_tas_lock(std::size_t* f) : lock_data(global_size_t(f)) {
				// #endif
				global_tas_lock(internal_field_type* f) : lock_data(global_lock_type(f)) {
					*lock_data = internal_field_type(false);
				};

				/**
				 * @brief try to lock
				 * @return true if lock was successfully taken,
				 *         false otherwise
				 */
				bool try_lock() {
					auto old_data = backend::atomic::load(lock_data, atomic::memory_order::relaxed);
					if (is_locked(old_data)) return false;
					std::size_t self = backend::node_id();
					auto new_data = get_data(true, self);
					log.try_lock_initiate(get_location(), old_data, new_data); // TODO: determine exact function
					/* Note: At this point, any successful lock acquire (lock_data == new_data)
					 * can be reversed at recovery (to old_data). An unsuccessful lock
					 * acquire (lock_data != new_data) can be ignored. */
					bool success = backend::atomic::compare_exchange(lock_data, old_data, new_data, atomic::memory_order::relaxed);
					if (success) {
						log.lock_success(get_location()); // TODO: determine exact function
						/* Note: This call confirms that the lock was taken (and possibly informs the predecessor node).
						 * After this the revert should be new_data -> old_data (anything else is an error).
						 * It also comes with a responsibility to inform predecessors that a restore is complete.
						 */
						if (last_user(old_data) == self /* || last_user(old_data) == internal_field_type::no_user */) {
							// The test asks if the lock was previously held by the acquiring node or no-one.
							/* note: doing nothing here is only safe because we are using
							 *       an SC for DRF memory model in ArgoDSM.
							 *       When changing this to something more strict, e.g.
							 *       TSO, then here a write buffer ordering must be
							 *       enforced.
							 *       A trivial implementation would call a
							 *       self-downgrade (as release() does), but a
							 *       better implementation could be thought of
							 *       if a better write-buffer is also implemented.
							 * why: semantically, we acquire at the beginning of
							 *      a lock. Any OTHER node seeing changes from within
							 *      the critical section can could therefore deduce
							 *      which writes from before the critical section must
							 *      have been issued. As write buffers can be cleared at
							 *      any time without ordering guarantees, this may cause
							 *      problems depending on the memory model. Using
							 *      SC for DRF disallows making such deductions, as they
							 *      would imply a data race.
							 */

							/* note: here a node-local acquire synchronization is needed.
							 *       The global lock still has to function as a correct
							 *       lock locally, so semantically we must ensure proper
							 *       synchronization at least within this node.
							 */
							std::atomic_thread_fence(std::memory_order_acquire);
							/* note: When the lock were in the initial unlocked state (init)
							 *       a node-local acquire is enough *only if* the lock cannot
							 *       return to the initial unlocked state (init) wihtout
							 *       also causing an argo acquire on all nodes.
							 *       This is typically not the case, which is why that
							 *       condiction has been disabled for this node-local
							 *       acquire optimisation.
							 */
						} else /* if (last_user(old_data) != init && last_user(old_data) != self) */ {
							// The lock was previously held by another node.
							// TODO: Trigger node-wide release on previous owner (assuming there is one)
							// log.inform_predecessor(last_user(old_data)) // TODO: determine exact function
							/* Note: This function doesn't necessarily belong in the log.
							 *       It could also be part of the lock_success call,
							 *       which then would block until the other node acknowleges the lock. */
							backend::acquire();
						}
						return true;
					}
					else {
						log.lock_fail(get_location()); // TODO: determine exact function
						/* Note: This removes the lock from the log. */
						return false;
					}
				}

				/**
				 * @brief release the lock
				 */
				void unlock() {
					backend::release();
					std::size_t self = backend::node_id();
					auto new_data = get_data(false, self);
					log.unlock(get_location(), new_data); // TODO: determine exact function
					/* Note: This loggs an unlock. It can be reverted by setting
					 * the lock bit (this assumes a lock is never unlocked by
					 * a node that doesn't have the lock).
					 * If another node (not self) takes the lock before the
					 * unlock is committed, that node will send a message which
					 * will be logged with the unlock entry. The other node should
					 * be informed when the unlock is committed and waited for
					 * when doing a recovery.
					 * 
					 * TODO: Is this enough? What what if the other node sends a
					 * message saying that it recovered a lock and it is taken
					 * to mean an earlier use of that lock at the receiver.
					 * Perhaps it is necessary to attach the group ID the
					 * (un)lock is logged to in the lock_data.
					 */
					backend::atomic::store(lock_data, new_data);
				}

				/**
				 * @brief take the lock
				 */
				void lock() {
					log.lock_initiate(get_location()); // TODO: determine exact function
					/* Note: This simply allocates a log entry that should remain
					 * even on failure (i.e. when lock_fail() is called).
					 * The assumption is that new attempts will be made until
					 * the lock succedes.
					 */
					while(!try_lock())
						std::this_thread::yield();
				}

		};
	} // namespace globallock
} // namespace argo

#endif /* argo_global_tas_lock_hpp */
