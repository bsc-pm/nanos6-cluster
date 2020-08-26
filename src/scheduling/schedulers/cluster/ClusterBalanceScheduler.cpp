/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include <vector>

#include <algorithm>
#include "ClusterBalanceScheduler.hpp"
#include "scheduling/Scheduler.hpp"
#include "memory/directory/Directory.hpp"
#include "system/RuntimeInfo.hpp"
#include "tasks/Task.hpp"
#include "ClusterSchedulerInterface.hpp"

#include <ClusterManager.hpp>
#include <DataAccessRegistrationImplementation.hpp>
#include <ExecutionWorkflow.hpp>
#include <VirtualMemoryManagement.hpp>

int ClusterBalanceScheduler::getScheduledNode(
	Task *task,
	ComputePlace *computePlace  __attribute__((unused)),
	ReadyTaskHint hint  __attribute__((unused))
) {
	const size_t clusterSize = ClusterManager::clusterSize();

	std::vector<size_t> bytes(clusterSize, 0);
	bool canBeOffloaded = true;

	/*
	 * Choose best node according to locality scheduler
	 */
	DataAccessRegistration::processAllDataAccesses(
		task,
		[&](const DataAccess *access) -> bool {

			const MemoryPlace *location = access->getLocation();
			if (location == nullptr) {
				assert(access->isWeak());
				location = Directory::getDirectoryMemoryPlace();
			}

			DataAccessRegion region = access->getAccessRegion();
			if (!VirtualMemoryManagement::isClusterMemory(region)) {
				canBeOffloaded = false;
				return false;
			}

			if (location->isDirectoryMemoryPlace()) {
				/* Read the location from the directory */
				const Directory::HomeNodesArray *homeNodes = Directory::find(region);

				for (const auto &entry : *homeNodes) {
					location = entry->getHomeNode();

					const size_t nodeId = getNodeIdForLocation(location);

					DataAccessRegion subregion = region.intersect(entry->getAccessRegion());
					bytes[nodeId] += subregion.getSize();
				}

				delete homeNodes;
			} else {
				const size_t nodeId = getNodeIdForLocation(location);
				bytes[nodeId] += region.getSize();
			}

			return true;
		}
	);

	if (!canBeOffloaded) {
		return nanos6_cluster_no_offload;
	}

	assert(!bytes.empty());
	std::vector<size_t>::iterator it = bytes.begin();
	const size_t bestNodeId = std::distance(it, std::max_element(it, it + clusterSize));

	ClusterNode *bestNode = ClusterManager::getClusterNode(bestNodeId);

	int numTasksAlready = (bestNode == ClusterManager::getCurrentClusterNode()) ?
							getNumLocalReadyTasks() :
							bestNode->getNumOffloadedTasks();

	// Schedule immediately to best node as long as queue not too long
	int coresPerNode = CPUManager::getTotalCPUs();
	if (numTasksAlready <= 2 * coresPerNode) {

		return bestNodeId;
	}

	// Otherwise schedule immediately to a non-loaded node
	int thiefTasksAlready = INT_MAX;
	ClusterNode *thief = nullptr;
	for (ClusterNode *node : ClusterManager::getClusterNodes()) {
		numTasksAlready = (node == ClusterManager::getCurrentClusterNode()) ?
								getNumLocalReadyTasks() :
								node->getNumOffloadedTasks();

		if (numTasksAlready < thiefTasksAlready) {
			thiefTasksAlready = numTasksAlready;
			thief = node;
		}
	}

	if (!thief) {
		thief = ClusterManager::getCurrentClusterNode();
	}

	int thiefId = thief->getIndex();
	if (thiefTasksAlready <= 2 * coresPerNode) {
		// offload immediately
		return thiefId;
	}

	// Otherwise put it in the queue for the best node; may schedule
	// there or steal the task later.
	std::lock_guard<SpinLock> guard(_readyQueueLock);
	_readyQueues[bestNodeId].push_back( new StealableTask(task, bytes) );

	// Don't schedule it yet; it is held here
	return nanos6_cluster_no_schedule;
}

Task *ClusterBalanceScheduler::stealTask(ClusterNode *targetNode)
{
	std::lock_guard<SpinLock> guard(_readyQueueLock);
	int targetNodeId = targetNode->getIndex();

	// Try to get from the node's queue
	if (!_readyQueues[targetNodeId].empty()) {
		StealableTask *stealableTask = _readyQueues[targetNodeId].front();
		_readyQueues[targetNodeId].pop_front();
		Task *task = stealableTask->_task;
		delete stealableTask;
		return task;
	}

	// Otherwise steal from another node's queue
	size_t victimNodeId = 0;
	float victimFrac = -1;
	StealableTask *victim = nullptr;
	std::list<StealableTask *>::const_iterator victimIt;

	for (size_t i = 0; i < _readyQueues.size(); i++) {
		for(std::list<StealableTask*>::const_iterator it=_readyQueues[i].begin(); it != _readyQueues[i].end(); it++) {
			float numFrac = (*it)->_affinityFrac[targetNodeId];
			if (numFrac > victimFrac) {
				victimFrac = numFrac;
				victimNodeId = i;
				victimIt = it;
				victim = *it;
			}
		}
	}
	if (victim) {
		Task *task = victim->_task;
		_readyQueues[victimNodeId].erase(victimIt);
		delete victim;
		return task;
	}
	return nullptr;
}

void ClusterBalanceScheduler::checkSendMoreAllNodes()
{
	int coresPerNode = CPUManager::getTotalCPUs();
	for (ClusterNode *node : ClusterManager::getClusterNodes()) {
		int numTasksAlready = (node == ClusterManager::getCurrentClusterNode()) ?
								getNumLocalReadyTasks() :
								node->getNumOffloadedTasks();
		int toSend = 2 * coresPerNode - numTasksAlready;
		for (int i = 0 ; i < toSend; i++) {
			Task *task = stealTask(node);
			if (task) {
				Scheduler::addReadyLocalOrExecuteRemote(node->getIndex(), task, nullptr, NO_HINT);
			} else {
				// No more tasks to send
				return;
			}
		}
	}
}

Task *ClusterBalanceScheduler::stealTask(ComputePlace *)
{
	// Steal a task from one of the node's ready queues
	// NOTE: steal task will decrease NumReadyTasks if successful.
	Task *task = stealTask(ClusterManager::getCurrentClusterNode());
	if (task) {
		checkSendMoreAllNodes();
	}
	return task;
}

void ClusterBalanceScheduler::offloadedTaskFinished(ClusterNode *remoteNode)
{
	int numRequestedTasks = 2;
	int totalLocalTasksHere = getNumLocalReadyTasks();
	int alreadyOffloaded = remoteNode->getNumOffloadedTasks(); // Total number of tasks already offloaded to that node

	// Do not give the remote node proportionally more work than we could potentially execute:
	//
	//       alreadyOffloaded + numSent < totalLocalTasksHere - numSent
	//
	//  i.e.
	//
	//                                     totalLocalTasksHere
	//                          numSent <  --------------------
	//                                     2 * alreadyOffloaded

	int	maxToSend = numRequestedTasks;
	if (alreadyOffloaded > 0) {
		maxToSend = std::min<int>(numRequestedTasks, totalLocalTasksHere / (2*alreadyOffloaded));
	}

	for (int i=0; i < maxToSend; i++) {
		Task *task = stealTask(remoteNode);
		if (task) {
			Scheduler::addReadyLocalOrExecuteRemote(remoteNode->getIndex(), task, nullptr, NO_HINT);
		} else {
			break;
		}
	}
}


