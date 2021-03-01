/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include "ClusterSchedulerInterface.hpp"

#include "ClusterLocalityScheduler.hpp"
#include "ClusterRandomScheduler.hpp"

#include <ClusterManager.hpp>

ClusterSchedulerInterface::ClusterSchedulerInterface(nanos6_cluster_scheduler_t id)
	: _thisNode(ClusterManager::getCurrentClusterNode()),
	_lastScheduledNode(nullptr),
	_schedulerMap(),
	_defaultScheduler(getOrCreateScheduler(id))
{
	assert(_defaultScheduler != nullptr);
	RuntimeInfo::addEntry("cluster-scheduler", "Cluster Scheduler", _defaultScheduler->getName());
}

void ClusterSchedulerInterface::addReadyLocalOrExecuteRemote(
	int nodeId,
	Task *task,
	ComputePlace *computePlace,
	ReadyTaskHint hint
) {
	assert(nodeId >= 0 || nodeId == nanos6_cluster_no_offload);

	ClusterNode * const targetNode =
		(nodeId == nanos6_cluster_no_offload) ?
		_thisNode :
		ClusterManager::getClusterNode(nodeId);

	assert(targetNode != nullptr);

	if (targetNode == _thisNode) {
		SchedulerInterface::addReadyTask(task, computePlace, hint);
		return;
	}

	ClusterMemoryNode *memoryNode = targetNode->getMemoryNode();
	assert(memoryNode != nullptr);

	ExecutionWorkflow::executeTask(task, targetNode, memoryNode);
}
