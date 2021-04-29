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
#include "ClusterHybridManager.hpp"
#include <DataAccessRegistrationImplementation.hpp>
#include <ExecutionWorkflow.hpp>
#include <VirtualMemoryManagement.hpp>
#include "executors/threads/cpu-managers/dlb/DLBCPUActivation.hpp"
#include "DLBCPUActivation.hpp"
#include "ClusterHybridMetrics.hpp"
#include "ClusterUtil.hpp"

// #define DMALLOC_FROM_DIRECTORY

int ClusterBalanceScheduler::getScheduledNode(
	Task *task,
	ComputePlace *computePlace  __attribute__((unused)),
	ReadyTaskHint hint  __attribute__((unused))
) {
	const size_t clusterSize = ClusterManager::clusterSize();

	std::vector<size_t> bytes(clusterSize, 0);
	bool canBeOffloaded = true;

	/*
	 * Schedule the task on the node that is home for most of its data
	 */
	DataAccessRegistration::processAllDataAccesses(
		task,
		[&](const DataAccess *access) -> bool {

			DataAccessRegion region = access->getAccessRegion();
				
			/* Cannot offload a task whose data is not all cluster memory */
			if (!VirtualMemoryManagement::isClusterMemory(region)) {
				canBeOffloaded = false;
				return false;  /* don't continue with other accesses */
			}

			bool readFromDirectory = true;

#if DMALLOC_FROM_DIRECTORY
			bool isDistributedRegion = VirtualMemoryManagement::isDistributedRegion(region);
			bool tryCurrentLocation = !isDistributedRegion;
#else
			bool tryCurrentLocation = true;
#endif
			if (tryCurrentLocation) {
				/* Not distributed region: use current location */
				const MemoryPlace *location = access->getLocation();
				if (location == nullptr) {
					assert(access->isWeak());
				} else {
					const size_t nodeId = getNodeIdForLocation(location);
					bytes[nodeId] += region.getSize();
					readFromDirectory = false;
				}
			}
			if (readFromDirectory) {
				/* Read the location from the directory */
				const Directory::HomeNodesArray *homeNodes = Directory::find(region);

				for (const auto &entry : *homeNodes) {
					/* Each entry has a location and a region */
					const MemoryPlace *location = entry->getHomeNode();
					const DataAccessRegion &homeNodeRegion = entry->getAccessRegion();

					/* Find the intersection with the data access region */
					DataAccessRegion subregion = region.intersect(homeNodeRegion);

					/* Count the bytes at the relevant node ID*/
					const size_t nodeId = getNodeIdForLocation(location);
					bytes[nodeId] += subregion.getSize();
				}

				delete homeNodes;
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
							ClusterHybridMetrics::getNumReadyTasks() :
							bestNode->getNumOffloadedTasks();

	// Schedule immediately to correct node as long as queue not too long
	if (numTasksAlready <= 2 * bestNode->getCurrentAllocCores()) {

		// If it is executed remotely then it cannot be stolen any more
		// clusterCout << "Direct schedule to " << bestNode->getIndex() << " [" << bestNode << " / " << ClusterManager::getCurrentClusterNode()
	// 				<< "] " << numTasksAlready << " vs " << (2*bestNode->getCurrentAllocCores()) << "\n";
		if (bestNode != ClusterManager::getCurrentClusterNode()) {
			ClusterHybridMetrics::incDirectOffload(1);
		}
		return bestNodeId;
	}

	// Otherwise find the node that has the least work as a proportion of its
	// allocation.
	float thiefRatio = 0.0;
	ClusterNode *thief = nullptr;
	for (ClusterNode *node : ClusterManager::getClusterNodes()) {
		numTasksAlready = (node == ClusterManager::getCurrentClusterNode()) ?
								ClusterHybridMetrics::getNumReadyTasks() :
								node->getNumOffloadedTasks();
		if (node->getCurrentAllocCores() > 0) {
			float ratio = numTasksAlready / node->getCurrentAllocCores();

			if (!thief || ratio < thiefRatio) {
				thiefRatio = ratio;
				thief = node;
			}
		}
	}

	if (!thief) {
		thief = ClusterManager::getCurrentClusterNode();
	}

	int thiefId = thief->getIndex();
	if (thiefRatio <= 2.0) {
		// offload immediately

		// If it is executed remotely then it cannot be stolen any more
		if (thiefId != ClusterManager::getCurrentClusterNode()->getIndex()) {
			ClusterHybridMetrics::incDirectThiefOffload(1);
		}
		Scheduler::addReadyLocalOrExecuteRemote(thiefId, task, computePlace, hint);
	} else {
		// Put in a ready queue (to take when we get the MessageTaskFinished)
		std::lock_guard<SpinLock> guard(_readyQueueLock);

		// Total ready tasks that could be stolen
		ClusterHybridMetrics::incNumReadyTasks(1);
		_readyQueues[thiefId].push_back( new StealableTask(task, bytes) );
	}
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
		ClusterHybridMetrics::incNumReadyTasks(-1);
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
		ClusterHybridMetrics::incNumReadyTasks(-1);
		delete victim;
		return task;
	}
	return nullptr;
}

void ClusterBalanceScheduler::checkSendMoreAllNodes()
{
	for (ClusterNode *node : ClusterManager::getClusterNodes()) {
		int alloc = node->getCurrentAllocCores();
		if (alloc > 0) {
			int numTasksAlready = (node == ClusterManager::getCurrentClusterNode()) ?
									ClusterHybridMetrics::getNumReadyTasks() :
									node->getNumOffloadedTasks();
			int toSend = 2 * alloc - numTasksAlready;
			for (int i = 0 ; i < toSend; i++) {
				Task *task = stealTask(node);
				if (task) {
					if (node != ClusterManager::getCurrentClusterNode()) {
						ClusterHybridMetrics::incSendMoreOffload(1);
					}
					Scheduler::addReadyLocalOrExecuteRemote(node->getIndex(), task, nullptr, NO_HINT);
				} else {
					// No more tasks to send
					return;
				}
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
	int totalTasksHere = ClusterHybridMetrics::getNumReadyTasks(); // Total number of ready and immovable and promised tasks here
	int alreadyOffloaded = remoteNode->getNumOffloadedTasks(); // Total number of tasks already offloaded to that node
	int localCores = DLBCPUActivation::getCurrentActiveOwnedCPUs();
	int remoteCores = remoteNode->getCurrentAllocCores();                               // Their allocation

	// Do not give the remote node proportionally more work than we could potentially execute:
	//
	//       (alreadyOffloaded + numSent)      totalTasksHere - numSent
	//       ----------------------------   <  ------------------------
	//             remoteCores                       localCores  
	//
	//                /        1              1        \    totalTasksHere     alreadyOffloaded
	//        numSent |  -----------  +   ----------   |  < --------------  -  ----------------
	//                \  remoteCores      localCores   /      localCores          remoteCores

	int maxToSend;
	if (localCores > 1) {
		// Assume one local core may be busy handling messages
		double rhs = (totalTasksHere * 1.0 / (localCores-1)) - (alreadyOffloaded * 1.0 / remoteCores);
		maxToSend = rhs / (1.0 / remoteCores + 1.0 / (localCores-1));
		Instrument::emitClusterEvent(Instrument::ClusterEventType::OffloadLimit, maxToSend);
		Instrument::emitClusterEvent(Instrument::ClusterEventType::OffloadHeadroom, std::max<int>(0,maxToSend-numRequestedTasks) );
		maxToSend = std::min<int>(numRequestedTasks, maxToSend);
	} else {
		maxToSend = numRequestedTasks;
	}

	// std::lock_guard<SpinLock> guard(_requestLock);
	int numSentTasks = 0;
	for (int i=0; i < maxToSend; i++) {
		Task *task = stealTask(remoteNode);
		if (task) {
			numSentTasks ++;
			Scheduler::addReadyLocalOrExecuteRemote(remoteNode->getIndex(), task, nullptr, NO_HINT);
			if (remoteNode != ClusterManager::getCurrentClusterNode()) {
				ClusterHybridMetrics::incCheckOffload(1);
			}
		} else {
			break;
		}
	}
}


