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
		__attribute__((unused)) bool forceHybrid)
	{}

	static bool inHybridClusterMode()
	{
		return false;
	}
};

#endif /* CLUSTER_HYBRID_MANAGER_HPP */
