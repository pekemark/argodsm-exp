/**
 * @file
 * @brief This file provides unit tests for accessing ArgoDSM memory in various ways
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#include "argo.hpp"
#include "backend/backend.hpp"
#include "gtest/gtest.h"

#include "backend/mpi/persistence.hpp"

/** @brief ArgoDSM memory size */
constexpr std::size_t size = 1<<28;
/** @brief ArgoDSM cache size */
constexpr std::size_t cache_size = size/2;

namespace mem = argo::mempools;
extern mem::global_memory_pool<>* default_global_mempool;

/**
 * @brief Class for the gtests fixture tests. Will reset the allocators to a clean state for every test
 */
class UninitializedAccessTest : public testing::Test {

	protected:
		UninitializedAccessTest() {
			argo_reset();
			argo::backend::persistence::commit_barrier(&argo::barrier, 1UL);
		}

		~UninitializedAccessTest() {
			argo::backend::persistence::commit_barrier(&argo::barrier, 1UL);
		}
};

/**
 * @brief Unittest that checks that there is no error when reading uninitialized coallocated memory.
 * @note this must be the first test, otherwise the memory is already "used"
 */
TEST_F(UninitializedAccessTest, ReadUninitializedSinglenode) {
	std::size_t allocsize = default_global_mempool->available();
	char *tmp = static_cast<char*>(collective_alloc(allocsize));
	if(argo::node_id() == 0) {
		for(std::size_t i = 0; i < allocsize; i++) {
			ASSERT_NO_THROW(asm volatile ("" : "=m" (tmp[i]) : "r" (tmp[i])));
		}
	}
}

/**
 * @brief The main function that runs the tests
 * @param argc Number of command line arguments
 * @param argv Command line arguments
 * @return 0 if success
 */
int main(int argc, char **argv) {
	argo::init(size, cache_size);
	persistence_registry.register_thread();
	::testing::InitGoogleTest(&argc, argv);
	auto res = RUN_ALL_TESTS();
	persistence_registry.unregister_thread();
	argo::finalize();
	return res;
}
