/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_SCHEDULER_INTERFACE_HPP
#define CLUSTER_SCHEDULER_INTERFACE_HPP

#include "scheduling/SchedulerInterface.hpp"
#include "system/RuntimeInfo.hpp"

#include <ClusterManager.hpp>

class ClusterSchedulerInterface : public SchedulerInterface {
protected:
	//! Current cluster node
	ClusterNode *_thisNode;
	ClusterNode *_lastScheduledNode;

	//! Number of cluster nodes
	int _clusterSize;

	//! Scheduler name
	const std::string _name;

	//! Function to pass the task to the local scheduler or call the execute function in workflow
	//! when the task is remote.
	void addReadyLocalOrExecuteRemote(
		size_t nodeId,
		Task *task,
		ComputePlace *computePlace,
		ReadyTaskHint hint);

	//! Handle constrains for cluster. Must return true only if some constrains where found and used
	//! properly.
	bool handleClusterSchedulerConstrains(Task *task, ComputePlace *computePlace, ReadyTaskHint hint);

	void addLocalReadyTask(Task *task, ComputePlace *computePlace, ReadyTaskHint hint = NO_HINT)
	{
		_lastScheduledNode = _thisNode;
		SchedulerInterface::addReadyTask(task, computePlace, hint);
	}

public:
	ClusterSchedulerInterface(const std::string &name)
		: _thisNode(ClusterManager::getCurrentClusterNode()),
		_lastScheduledNode(nullptr),
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
