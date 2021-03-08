/**
 * @file
 * @brief This file implements the cyclic data distribution
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#ifndef argo_cyclic_distribution_hpp
#define argo_cyclic_distribution_hpp argo_cyclic_distribution_hpp

#include "base_distribution.hpp"

namespace argo {
	namespace data_distribution {
		/**
		 * @brief the cyclic data distribution
		 * @details linearly distributes a block of pages per round in a round-robin fashion.
		 */
		template<int instance>
		class cyclic_distribution : public base_distribution<instance> {
			public:
				virtual node_id_t homenode (char* const ptr) {
					static const std::size_t pageblock = env::allocation_block_size() * granularity;
					const std::size_t addr = (ptr - base_distribution<instance>::start_address) / granularity * granularity;
					const std::size_t pagenum = addr / pageblock;
					const node_id_t homenode = pagenum % base_distribution<instance>::nodes;

					if(homenode >= base_distribution<instance>::nodes) {
						exit(EXIT_FAILURE);
					}
					return homenode;
				}

				virtual std::size_t local_offset (char* const ptr) {
					static const std::size_t pageblock = env::allocation_block_size() * granularity;
					const std::size_t drift = (ptr - base_distribution<instance>::start_address) % granularity;
					const std::size_t addr = (ptr - base_distribution<instance>::start_address) / granularity * granularity;
					const std::size_t pagenum = addr / pageblock;
					const std::size_t offset = pagenum / base_distribution<instance>::nodes * pageblock + addr % pageblock + drift;
					
					if(offset >= static_cast<std::size_t>(base_distribution<instance>::size_per_node)) {
						exit(EXIT_FAILURE);
					}
					return offset;
				}
		};
	} // namespace data_distribution
} // namespace argo

#endif /* argo_cyclic_distribution_hpp */
