/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include "ClusterSchedulerInterface.hpp"

#include "ClusterLocalityScheduler.hpp"
#include "ClusterRandomScheduler.hpp"

#include <ClusterManager.hpp>

ClusterSchedulerInterface::ClusterSchedulerInterface()
	: _thisNode(ClusterManager::getCurrentClusterNode()),
	  _lastScheduledNode(nullptr),
	  _defaultScheduler(new ClusterLocalityScheduler(this))
{
	assert(_defaultScheduler != nullptr);
	RuntimeInfo::addEntry("cluster-scheduler", "Cluster Scheduler", _defaultScheduler->getName());
}


bool ClusterSchedulerInterface::handleClusterSchedulerConstrains(
	Task *task,
	ComputePlace *computePlace,
	ReadyTaskHint hint
) {
	//! We do not offload spawned functions, if0 tasks, remote task
	//! and tasks that already have an ExecutionWorkflow created for
	//! them
	if (task->isSpawned()             // Don't offload spawned tasks.
		|| task->isRemoteWrapper()    // This will save the day when we want offload spawned tasks
		|| task->isRemoteTask()       // Already offloaded don't re-offload
		|| task->isIf0()
		|| task->isPolling()          // Polling tasks
		|| task->isTaskloop()         // for now don't offload task{loop,for}
		|| task->isTaskfor()
		|| task->getWorkflow() != nullptr) {
		addLocalReadyTask(task, computePlace, hint);
		return true;
	}

	if (task->hasConstrains()) {
		const int nodeId = task->getNode();
		FatalErrorHandler::failIf(
			nodeId >= ClusterManager::clusterSize(),
			"node in node() constraint out of range"
		);

		if (nodeId != nanos6_cluster_no_offload) {
			addReadyLocalOrExecuteRemote(nodeId, task, computePlace, hint);
			return true;
		}
	}

	return false;
}

void ClusterSchedulerInterface::addReadyLocalOrExecuteRemote(
	size_t nodeId,
	Task *task,
	ComputePlace *computePlace,
	ReadyTaskHint hint
) {
	ClusterNode *targetNode = ClusterManager::getClusterNode(nodeId);
	assert(targetNode != nullptr);

	if (targetNode == _thisNode) {
		addLocalReadyTask(task, computePlace, hint);
		return;
	}

	ClusterMemoryNode *memoryNode = targetNode->getMemoryNode();
	assert(memoryNode != nullptr);

	ExecutionWorkflow::executeTask(task, targetNode, memoryNode);
}
