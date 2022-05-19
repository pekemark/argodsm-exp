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
#include "../backend/mpi/statistics.hpp"
#include "../backend/mpi/persistence.hpp"

namespace argo {
	namespace globallock {
		/** @brief a global test-and-set lock */
		class global_tas_lock {
			private:
				class lock_repr_standard {
					public:
						/** @brief lock representation type this class supports */
						using lock_repr_type = std::uint64_t;
					private:
						/** @brief constant signifying lock is in an initial state and free */
						static const lock_repr_type init = -2;
						/** @brief constant signifying lock is taken */
						static const lock_repr_type locked = -1;

						/** @brief internal alias for determining node ID */
						static argo::node_id_t inline self() { return backend::node_id(); }

					public:
						// Methods for information extraction. Side-effect free.

						/** @brief Determines whether the lock field represents the an initial state.
						 * @param lock_field The lock field to inspect.
						 * @return True if the lock field is in an initial state, otherwise false.
						 */
						static bool inline is_init(lock_repr_type lock_field) { return lock_field == init; }

						/** @brief Determines whether the lock field represents the a locked state.
						 * @param lock_field The lock field to inspect.
						 * @return True if the lock field is in a locked state, otherwise false.
						 */
						static bool inline is_locked(lock_repr_type lock_field) { return lock_field == locked; }

						/** @brief Extracts the user information in the lock_field.
						 * Undefined for a lock field in an initial state.
						 * @param lock_field The lock field to inspect.
						 * @return The node ID of the user encoded in the lock field.
						 */
						static argo::node_id_t inline get_user(lock_repr_type lock_field) { return lock_field; }

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

						/** @brief Creates a lock field in a locked state.
						 * @return A lock field in a locked state.
						 */
						static lock_repr_type inline make_locked() { return locked; }

						/** @brief Creates a lock field in an unlocked state.
						 * @param node Node ID to encode as the user of the lock.
						 * @return A lock field in an unlocked state, encoded with the specified user.
						 */
						static lock_repr_type inline make_unlocked(argo::node_id_t node) { return node; }

						/** @brief Creates a lock field in an unlocked state.
						 * @return A lock field in an unlocked state, encoded with the local node as the user.
						 */
						static lock_repr_type inline make_unlocked() { return make_unlocked(self()); }

						/** @brief Creates a lock field in an initial state.
						 * @return A lock field in an initial state.
						 */
						static lock_repr_type inline make_init() { return init; }


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

				using lock_repr = backend::persistence::lock_repr;

			public:
				/**
				 * @brief internally used type for lock field
				 * @note this type may change without warning,
				 *       user code must use this type alias
				 */
				using internal_field_type = lock_repr::lock_repr_type;

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
				global_lock_type lock_field;

			public:
				/**
				 * @brief construct global tas lock from existing memory in global address space
				 * @param f pointer to global field for storing lock state
				 */
				global_tas_lock(internal_field_type* f) : lock_field(global_lock_type(f)) {
					// if (lock_field.node() == backend::node_id())
					// 	backend::atomic::store(lock_field, lock_repr::make_init(), atomic::memory_order::relaxed);
					*lock_field = lock_repr::make_init();
					/* TODO: Every node has its own TAS lock with the same underlying data.
					 * On creation, a single node should atomically write the initial value.
					 * The choice of node can be a fixed ID, alternatively the homenode,
					 * assuming all nodes are participating. Failing this assumption,
					 * a correct fallback would be to have all nodes atomically write
					 * the initial value. However, atomic writes, even from only the home node,
					 * appear to be very costly. A more performant, but technically incorrect
					 * solution is to have all nodes non-atomically write the initial value.
					 * This should be fine as long as there is a barrier between initialisation and first use.
					 */
				};

				/**
				 * @brief try to lock
				 * @return true if lock was successfully taken,
				 *         false otherwise
				 */
				bool try_lock() {
					auto old_field = backend::atomic::load(lock_field, atomic::memory_order::relaxed);
					if (lock_repr::is_locked(old_field)) return false;
					argo::node_id_t self = backend::node_id();
					auto new_field = lock_repr::try_lock_initiate(lock_field.get(), old_field);
					bool success = backend::atomic::compare_exchange(lock_field, old_field, new_field, atomic::memory_order::relaxed);
					if (success) {
						lock_repr::lock_success(lock_field.get());
						if (lock_repr::is_user_self(old_field) || lock_repr::is_init(old_field)) {
							// The lock was not previously held by another node.
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
							 */
						} else {
							// The lock was previously held by another node.
							// TODO: Trigger node-wide release on previous owner (assuming there is one)
							backend::acquire();
							stats.locktransfers++;
						}
						return true;
					}
					else {
						lock_repr::lock_fail(lock_field.get());
						return false;
					}
				}

				/**
				 * @brief release the lock
				 */
				void unlock() {
					backend::release();
					auto new_field = lock_repr::unlock(lock_field.get());
					backend::atomic::store(lock_field, new_field);
				}

				/**
				 * @brief take the lock
				 */
				void lock() {
					lock_repr::lock_initiate(lock_field.get());
					while(!try_lock())
						std::this_thread::yield();
				}

		};
	} // namespace globallock
} // namespace argo

#endif /* argo_global_tas_lock_hpp */
