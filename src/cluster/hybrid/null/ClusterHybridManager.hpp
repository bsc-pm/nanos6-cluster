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
		__attribute__((unused)) int apprankNum)
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
};

#endif /* CLUSTER_HYBRID_MANAGER_HPP */
