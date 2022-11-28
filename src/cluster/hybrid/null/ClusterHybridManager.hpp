/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_HYBRID_MANAGER_HPP
#define CLUSTER_HYBRID_MANAGER_HPP

#include <vector>
#include "ClusterManager.hpp"
#include "executors/threads/CPUManager.hpp"

class ClusterHybridManager {


public:

	static void preinitialize(
		__attribute__((unused)) bool forceHybrid,
		__attribute__((unused)) int externalRank,
		__attribute__((unused)) int apprankNum,
		__attribute__((unused)) int internalRank,
		__attribute__((unused)) int physicalNodeNum,
		__attribute__((unused)) int indexThisPhysicalNode,
		__attribute__((unused)) size_t clusterSize,
		__attribute__((unused)) const std::vector<int> &internalRankToExternalRank,
		__attribute__((unused)) const std::vector<int> &instanceThisNodeToExternalRank)
	{}

	static void initialize()
	{
		int clusterSize = ClusterManager::clusterSize();

		// Without hybrid mode: set one core per ClusterNode. This allocation
		// is only important for the dmalloc distribution policy.  The
		// distribution policy distributes the array over the total number of
		// cores across all ranks. If we had set the allocated number of cores
		// to the actual number of cores from CPUManager::getTotalCPUs(), a
		// perfect distribution across the nodes would require the number of
		// elements to be a multiple of the total number of cores in the
		// program.  "Faking" the number of cores to equal 1 means that a
		// perfect distribution requires the number of elements only to be a
		// multiple of the number of cluster nodes (processes).
		for (int i = 0; i < clusterSize; i++) {
			ClusterManager::getClusterNode(i)->setCurrentAllocCores(1);
		}
	}

	static bool inHybridClusterMode()
	{
		return false;
	}

	static int getCurrentOwnedCPUs()
	{
		return CPUManager::getTotalCPUs();
	}

	static int getCurrentOwnedOrGivingCPUs()
	{
		return CPUManager::getTotalCPUs();
	}

	static int getCurrentLentOwnedCPUs()
	{
		return 0;
	}

	static int getCurrentBorrowedCPUs()
	{
		return 0;
	}

	static int getCurrentActiveOwnedCPUs()
	{
		return CPUManager::getTotalCPUs();
	}

};

#endif /* CLUSTER_HYBRID_MANAGER_HPP */
