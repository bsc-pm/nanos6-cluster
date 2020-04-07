/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_SCHEDULER_INTERFACE_HPP
#define CLUSTER_SCHEDULER_INTERFACE_HPP

#include "scheduling/SchedulerInterface.hpp"
#include "system/RuntimeInfo.hpp"

#include <ClusterManager.hpp>

class ClusterSchedulerInterface : public SchedulerInterface {
protected:
	//! Current cluster node
	const ClusterNode *_thisNode;

	//! Number of cluster nodes
	int _clusterSize;

	//! Scheduler name
	const std::string _name;

public:
	ClusterSchedulerInterface(const std::string &name)
		: _thisNode(ClusterManager::getCurrentClusterNode()),
		  _clusterSize(ClusterManager::clusterSize()),
		  _name(name)
	{
		RuntimeInfo::addEntry("cluster-scheduler", "Cluster Scheduler", _name);
	}

	virtual ~ClusterSchedulerInterface()
	{
	}

	inline std::string getName() const
	{
		return _name;
	}

	virtual void addReadyTask(Task *task, ComputePlace *computePlace, ReadyTaskHint hint = NO_HINT) = 0;
};

#endif // CLUSTER_SCHEDULER_INTERFACE_HPP
