/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "memory/directory/Directory.hpp"
#include "DistributionPolicy.hpp"
#include "hardware/places/MemoryPlace.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "memory/numa/NUMAManager.hpp"

#include <ClusterManager.hpp>
#include <DataAccessRegion.hpp>
#include <DataAccessRegistration.hpp>

#include "ClusterMemoryManagement.hpp"

namespace ClusterDirectory {
	static void registerAllocationEqupart(DataAccessRegion const &region, size_t clusterSize)
	{
		assert(clusterSize > 0);

		char *ptr = (char *)region.getStartAddress();
		size_t size = region.getSize();

		size_t blockSize = size / clusterSize;
		size_t residual = size % clusterSize;
		size_t numBlocks = (blockSize > 0) ? clusterSize : 0;

		for (size_t i = 0; i < numBlocks; ++i) {
			DataAccessRegion newRegion((void *)ptr, blockSize);
			ClusterMemoryNode *homeNode = ClusterManager::getMemoryNode(i);
			Directory::insert(newRegion, homeNode);
			ptr += blockSize;

			if (homeNode == ClusterManager::getCurrentMemoryNode()) {
				// Use a blocked memory allocation for NUMA (this could be improved later)
				nanos6_bitmask_t bitmask;
				NUMAManager::setAnyActive(&bitmask);
				size_t numNumaAny = NUMAManager::countEnabledBits(&bitmask);
				assert(numNumaAny > 0);
				size_t blockSizeNUMA = newRegion.getSize() / numNumaAny;

				void *newPtr = newRegion.getStartAddress();
				size_t newSize = newRegion.getSize();
				NUMAManager::fullyIntersectPages(&newPtr, &newSize, &blockSizeNUMA);

				if (newSize > 0) {
					NUMAManager::setNUMAAffinity(newPtr, newSize, &bitmask, blockSizeNUMA);
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
		}
	}

	void registerAllocation(
		const ClusterMemoryManagement::DmallocDataInfo *dmallocInfo, Task *task, size_t clusterSize
	) {
		// If numa.tracking is set to "auto", then a dmalloc enables NUMA tracking on all nodes
		NUMAManager::enableTrackingIfAuto();

		if (task) {
			// Register local access. A location of nullptr means that the data is currently
			// uninitialized so the first access doesn't need a copy.
			DataAccessRegistration::registerLocalAccess(
				task, dmallocInfo->_region, /* location */ nullptr, /* isStack */ false
			);
		}

		switch (dmallocInfo->_policy) {
			case nanos6_equpart_distribution:
				assert(dmallocInfo->_nrDim == 0);

				registerAllocationEqupart(dmallocInfo->_region, clusterSize);
				break;
			case nanos6_block_distribution:
			case nanos6_cyclic_distribution:
			default:
				FatalErrorHandler::fail("Unknown distribution policy");
		}
	}

	void unregisterAllocation(DataAccessRegion const &region)
	{
		//! Erase from Directory
		Directory::erase(region);
		void *ptr = region.getStartAddress();
		size_t size = region.getSize();
		NUMAManager::fullyIntersectPages(&ptr, &size, nullptr);
		if (size > 0) {
			NUMAManager::unsetNUMAAffinity(ptr, size);
		}
	}
}
