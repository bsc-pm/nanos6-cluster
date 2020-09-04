/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_HYBRID_MANAGER_HPP
#define CLUSTER_HYBRID_MANAGER_HPP

#include <list>
#include <string.h>
#include "DataAccessRegion.hpp"
#include <nanos6/cluster.h>
#include <sched.h>

class ClusterHybridManager {

private:
	static bool _inHybridClusterMode;
	static int _numCPUs;

public:
	static void initialize(bool forceHybrid);

	static bool inHybridClusterMode()
	{
		return _inHybridClusterMode;
	}

	static void getInitialCPUMask(cpu_set_t *set);

	static int countEnabledCPUs(void);
};

#endif /* CLUSTER_HYBRID_MANAGER_HPP */
