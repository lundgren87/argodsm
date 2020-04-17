/**
 * @file
 * @brief This file provides unit tests for the ArgoDSM prefetch mechanism
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#include <iostream>

#include <limits.h>
#include <unistd.h>

#include "argo.hpp"
#include "allocators/generic_allocator.hpp"
#include "allocators/collective_allocator.hpp"
#include "allocators/null_lock.hpp"
#include "backend/backend.hpp"
#include "gtest/gtest.h"

/** @brief ArgoDSM memory size */
constexpr std::size_t size = 1<<30;
/** @brief ArgoDSM cache size */
constexpr std::size_t cache_size = size/8;

namespace mem = argo::mempools;
extern mem::global_memory_pool<>* default_global_mempool;

/** @brief ArgoDSM page size */
const unsigned int pagesize = 4096;

/** @brief A random int constant */
constexpr char c_const = 'a';

/**
 * @brief Class for the gtests fixture tests. Will reset the allocators to a clean state for every test
 */
class PrefetchTest : public testing::Test {

	protected:
		PrefetchTest() {
			argo_reset();
			argo::barrier();
		}

		~PrefetchTest() {
			argo::barrier();
		}
};



/**
 * @brief Unittest that checks that there is no error when accessing the first byte of the allocation.
 */
TEST_F(PrefetchTest, FirstPage) {
	std::size_t allocsize = default_global_mempool->available();
	char *tmp = static_cast<char*>(collective_alloc(allocsize));
	if(argo::node_id()==0){
		tmp[0] = c_const;
	}
	argo::barrier();
	ASSERT_EQ(c_const, tmp[0]);
}

/**
 * @brief Unittest that checks that there is no error when accessing the last page in memory.
 */
TEST_F(PrefetchTest, OutOfBounds) {
	std::size_t allocsize = default_global_mempool->available();
	char *tmp = static_cast<char*>(collective_alloc(allocsize));
	if(argo::node_id()==0){
		tmp[allocsize-1] = c_const;
	}
	argo::barrier();
	ASSERT_EQ(c_const, tmp[allocsize-1]);
}

/**
 * @brief Unittest that checks that there is no error when accessing a page that may already be prefetched by a previous access.
 */
TEST_F(PrefetchTest, AccessPrefetched) {
	std::size_t allocsize = default_global_mempool->available();
	char *tmp = static_cast<char*>(collective_alloc(allocsize));
	if(argo::node_id()==0){
		tmp[pagesize-1] = c_const;
		tmp[pagesize] = c_const;
	}
	argo::barrier();
	ASSERT_EQ(c_const, tmp[pagesize-1]);
	ASSERT_EQ(c_const, tmp[pagesize]);
}

/**
 * @brief The main function that runs the tests
 * @param argc Number of command line arguments
 * @param argv Command line arguments
 * @return 0 if success
 */
int main(int argc, char **argv) {
	argo::init(size, cache_size);
	::testing::InitGoogleTest(&argc, argv);
	auto res = RUN_ALL_TESTS();
	argo::finalize();
	return res;
}
