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

namespace argo {
	namespace globallock {
		/** @brief a global test-and-set lock */
		class global_tas_lock {
			private:
				class lock_data {
					public:
						static const argo::node_id_t init = -1;
						argo::node_id_t last_user;
						bool locked;
						lock_data(bool locked = false, argo::node_id_t last_user = init)
						: locked(locked), last_user(last_user) {};
						bool operator==(const lock_data &other) {
							return
								this->last_user == other.last_user &&
								this->locked == other.locked;
						}
						bool inline is_locked() { return locked; }
						bool inline get_last_user() { return last_user; }
				};

			public:
				/**
				 * @brief internally used type for lock field
				 * @note this type may change without warning,
				 *       user code must use this type alias
				 */
				using internal_field_type = lock_data;

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
				global_lock_type lock_data;

			public:
				/**
				 * @brief construct global tas lock from existing memory in global address space
				 * @param f pointer to global field for storing lock state
				 */
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
					if (old_data.is_locked()) return false;
					std::size_t self = backend::node_id();
					auto new_data = internal_field_type(true, self);
					bool success = backend::atomic::compare_exchange(lock_data, old_data, new_data, atomic::memory_order::relaxed);
					if (success) {
						if (old_data.get_last_user() == self || old_data.get_last_user() == internal_field_type::init) {
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
						} else /* if (old_data.get_last_user() != internal_field_type::init && old_data.get_last_user() != self) */ {
							// The lock was previously held by another node.
							// TODO: Trigger node-wide release on previous owner (assuming there is one)
							backend::acquire();
							stats.locktransfers++;
						}
						return true;
					}
					else {
						return false;
					}
				}

				/**
				 * @brief release the lock
				 */
				void unlock() {
					backend::release();
					std::size_t self = backend::node_id();
					auto new_data = internal_field_type(false, self);
					backend::atomic::store(lock_data, new_data);
				}

				/**
				 * @brief take the lock
				 */
				void lock() {
					while(!try_lock())
						std::this_thread::yield();
				}

		};
	} // namespace globallock
} // namespace argo

#endif /* argo_global_tas_lock_hpp */
