/**
 * @file
 * @brief This file provides unit tests for the ArgoDSM API functions
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#include <iostream>

#include <limits.h>
#include <unistd.h>

#include "argo.hpp"
#include "allocators/collective_allocator.hpp"
#include "backend/backend.hpp"
#include "env/env.hpp"
#include "data_distribution/data_distribution.hpp"
#include "gtest/gtest.h"

#include "backend/mpi/persistence.hpp"

/** @brief ArgoDSM memory size */
constexpr std::size_t size = 1<<26;
/** @brief ArgoDSM cache size */
constexpr std::size_t cache_size = size/8;

namespace env = argo::env;
namespace dd = argo::data_distribution;
namespace mem = argo::mempools;
extern mem::global_memory_pool<>* default_global_mempool;

/** @brief ArgoDSM page size */
const std::size_t page_size = 4096;

/** @brief A "random" char constant */
constexpr char c_const = 'a';

/**
 * @brief Class for the gtests fixture tests. Will reset the allocators to a clean state for every test
 */
class APITest : public testing::Test {

	protected:
		APITest() {
			argo_reset();
			argo::backend::persistence::commit_barrier(&argo::barrier, 1UL);
		}

		~APITest() {
			argo::backend::persistence::commit_barrier(&argo::barrier, 1UL);
		}
};



/**
 * @brief Unittest that checks correctness of the
 * argo::is_argo_address(T* addr) API function.
 */
TEST_F(APITest, IsArgoAddress) {
	std::size_t alloc_size = default_global_mempool->available();
	char *tmp = static_cast<char*>(collective_alloc(alloc_size));
	char *global_base = argo::backend::global_base();
	std::size_t global_size = argo::backend::global_size();
	ASSERT_FALSE(argo::is_argo_address(global_base-1));
	ASSERT_TRUE(argo::is_argo_address(global_base));
	ASSERT_TRUE(argo::is_argo_address(&tmp[0]));
	ASSERT_TRUE(argo::is_argo_address(&tmp[alloc_size/2]));
	ASSERT_TRUE(argo::is_argo_address(&tmp[alloc_size-1]));
	ASSERT_FALSE(argo::is_argo_address(global_base+global_size));
}

/**
 * @brief Unittest that checks correctness of the
 * argo::get_homenode(T* addr) API function.
 */
TEST_F(APITest, GetHomeNode) {
	auto ptrack = persistence_registry.get_tracker();
	std::size_t alloc_size = default_global_mempool->available();
	char *tmp = static_cast<char*>(collective_alloc(alloc_size));
	argo::node_id_t node_id = argo::node_id();
	std::size_t num_nodes = argo::number_of_nodes();
	char* start = argo::backend::global_base();
	char* end = start + argo::backend::global_size();

	/* Touch an equal number of pages per node */
	int first_writer = (argo::get_homenode(start)+1) % num_nodes;
	int my_start = (node_id-first_writer+num_nodes)%num_nodes;
	for(std::size_t s=page_size*my_start;
			s<alloc_size-1;
			s+=page_size*num_nodes) {
		tmp[s] = c_const;
		ptrack->join_apb();
	}
	argo::backend::persistence::commit_barrier(&argo::barrier, 1UL);

	/* Test that the number of pages owned by each node is equal */
	std::size_t counter = 0;
	std::vector<std::size_t> node_counters(num_nodes);
	for(char* c = start; c<end; c+=page_size) {
		node_counters[argo::get_homenode(c)]++;
		counter++;
	}
	for(std::size_t count : node_counters) {
		ASSERT_EQ(counter/num_nodes, count);
	}
}

/**
 * @brief Unittest that checks that argo::get_block_size() is
 * consistent with argo::env::block_size() for all cyclical
 * allocations.
 */
TEST_F(APITest, GetBlockSize) {
	std::size_t env_block_size = env::allocation_block_size();
	std::size_t api_block_size = argo::get_block_size();
	std::size_t size_per_node = argo::backend::global_size()/argo::number_of_nodes();
	if(dd::is_cyclic_policy()) {
		ASSERT_EQ(api_block_size, env_block_size*page_size);
	}else if(dd::is_first_touch_policy()){
		ASSERT_EQ(api_block_size, page_size);
	}else{
		ASSERT_EQ(api_block_size, size_per_node);
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
