/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_HYBRID_MANAGER_HPP
#define CLUSTER_HYBRID_MANAGER_HPP

#include "ClusterHybridInterface.hpp"

#include <list>
#include <string.h>
#include "DataAccessRegion.hpp"
#include <nanos6/cluster.h>
#include <sched.h>

class ClusterHybridManager {

private:
	static bool _inHybridClusterMode;
	static int _numCPUs;

	//! Cluster hybrid interface for coordination among appranks
	static ClusterHybridInterface *_hyb;

public:

	static void preinitialize(
		bool forceHybrid,
		int externalRank,
		int apprankNum,
		int internalRank,
		int physicalNodeNum,
		int indexThisPhysicalNode);

	static void initialize();

	static bool inHybridClusterMode()
	{
		return _inHybridClusterMode;
	}

	//! \brief In hybrid cluster mode, update numbers of cores per instance
	static void poll()
	{
		if(_hyb) {
			_hyb->poll();
		}
	}

	static void getInitialCPUMask(cpu_set_t *set);
};

#endif /* CLUSTER_HYBRID_MANAGER_HPP */
