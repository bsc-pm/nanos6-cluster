/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include "ClusterSchedulerInterface.hpp"

#include <ClusterManager.hpp>

bool ClusterSchedulerInterface::handleClusterSchedulerConstrains(
	Task *task,
	ComputePlace *computePlace,
	ReadyTaskHint hint
) {
	//! We do not offload spawned functions, if0 tasks, remote task
	//! and tasks that already have an ExecutionWorkflow created for
	//! them
	if (task->isSpawned() || task->isIf0() || task->isRemote() || task->getWorkflow() != nullptr) {
		addLocalReadyTask(task, computePlace, hint);
		return true;
	}

	if (task->hasConstrains()) {
		const size_t nodeId = task->getNode();
		FatalErrorHandler::failIf(
			(int)nodeId >= ClusterManager::clusterSize(),
			"node in node() constraint out of range"
		);

		if (nodeId != DEFAULT_NODE_VALUE) {
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
