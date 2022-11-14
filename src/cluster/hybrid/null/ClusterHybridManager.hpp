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
		int numCores = CPUManager::getTotalCPUs();
		for (int i = 0; i < clusterSize; i++) {
			ClusterManager::getClusterNode(i)->setCurrentAllocCores(numCores);
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