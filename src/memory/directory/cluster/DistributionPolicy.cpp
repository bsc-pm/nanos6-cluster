/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "memory/directory/Directory.hpp"
#include "DistributionPolicy.hpp"
#include "hardware/places/MemoryPlace.hpp"
#include "lowlevel/FatalErrorHandler.hpp"

#include <ClusterManager.hpp>
#include <DataAccessRegion.hpp>
#include <DataAccessRegistration.hpp>


namespace ClusterDirectory {
	static void registerAllocationEqupart(DataAccessRegion const &region)
	{
		size_t clusterSize = ClusterManager::clusterSize();

		std::vector<int> coresPerRank;
		coresPerRank.resize(clusterSize);
		//= ClusterManager::getCoresPerRank();
		int totalCores = 0;

		/* Get cores per rank and total number of cores */
		// std::cout << "Dmalloc: clusterSize = " << clusterSize << "\n";
		for (size_t node=0; node<clusterSize; node++)
		{
			int numCores = ClusterManager::getClusterNode(node)->getCurrentAllocCores();
			coresPerRank[node] = numCores;
			totalCores += numCores;
			// std::cout << "Dmalloc: coresPerRank[" << node << "] = " << numCores << "\n";
		}
		assert(totalCores > 0);

		/* Divide up the region by cores */
		void *address = region.getStartAddress();
		size_t size = region.getSize();
		size_t blockSize = size / totalCores;
		size_t residual = size % totalCores;
		// size_t numBlocks = (blockSize > 0) ? clusterSize : 0;

		char *ptr = (char *)address;
                if (blockSize > 0)
                {
                    for (size_t i = 0; i < clusterSize; ++i) {
                            int numBlocks = coresPerRank[i];
                            if (numBlocks > 0)
                            {
                                DataAccessRegion newRegion((void *)ptr, numBlocks * blockSize);
                                ClusterMemoryNode *homeNode = ClusterManager::getMemoryNode(i);
                                Directory::insert(newRegion, homeNode);
                                ptr += numBlocks * blockSize;
                            }
                    }
                }

		//! Add an extra entry to the first node for any residual
		//! uncovered region.
		if (residual > 0) {
			DataAccessRegion newRegion((void *)ptr, residual);
			ClusterMemoryNode *homeNode = ClusterManager::getMemoryNode(0);
			assert(homeNode != nullptr);

			Directory::insert(newRegion, homeNode);
			ptr += residual;
		}
		assert(ptr == (char*)address + size);
	}

	void registerAllocation(DataAccessRegion const &region,
			nanos6_data_distribution_t policy,
			__attribute__((unused)) size_t nrDimensions,
			__attribute__((unused)) size_t *dimensions,
			Task *task)
	{
		if (task) {
			// Register local access. A location of nullptr means that the data is currently
			// uninitialized so the first access doesn't need a copy.
			DataAccessRegistration::registerLocalAccess(task, region, /* location */ nullptr, /* isStack */ false);
		}

		switch (policy) {
			case nanos6_equpart_distribution:
				assert(nrDimensions == 0);
				assert(dimensions == nullptr);

				registerAllocationEqupart(region);
				break;
			case nanos6_block_distribution:
			case nanos6_cyclic_distribution:
			default:
				FatalErrorHandler::failIf(
					true,
					"Unknown distribution policy"
				);
		}
	}

	void unregisterAllocation(DataAccessRegion const &region)
	{
		//! Erase from Directory
		Directory::erase(region);
	}
}
