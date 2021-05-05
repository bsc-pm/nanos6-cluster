/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include <map>
#include <utility>
#include <vector>

#include <nanos6/task-instantiation.h>

#include "ClusterTaskContext.hpp"
#include "TaskOffloading.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "system/ompss/AddTask.hpp"
#include "tasks/Task.hpp"

#include <ClusterManager.hpp>
#include <RemoteTasksInfoMap.hpp>
#include <DataAccessRegistration.hpp>
#include <Directory.hpp>
#include <MessageReleaseAccess.hpp>
#include <MessageTaskFinished.hpp>
#include <MessageTaskNew.hpp>
#include "MessageSatisfiability.hpp"
#include <NodeNamespace.hpp>

#include <ClusterUtil.hpp>

namespace TaskOffloading {

	void propagateSatisfiability(Task *localTask, SatisfiabilityInfo const &satInfo)
	{
		assert(localTask != nullptr);
		assert(!satInfo.empty());

		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();

		CPU * const cpu = (currentThread == nullptr) ? nullptr : currentThread->getComputePlace();

		CPUDependencyData localDependencyData;
		CPUDependencyData &hpDependencyData =
			(cpu != nullptr) ? cpu->getDependencyData() : localDependencyData;

		if (!Directory::isDirectoryMemoryPlace(satInfo._src)) {
			MemoryPlace const *loc;
			if (satInfo._src == -1) {
				loc = nullptr; // -1 means nullptr: see comment in ClusterDataLinkStep::linkRegion().
			} else {
				loc = ClusterManager::getMemoryNode(satInfo._src);
			}
			DataAccessRegistration::propagateSatisfiability(
				localTask, satInfo._region, cpu,
				hpDependencyData, satInfo._readSat,
				satInfo._writeSat, loc
			);

			return;
		}

		// The access is in the Directory. Retrieve the home nodes and
		// propagate satisfiability per region
		Directory::HomeNodesArray *array = Directory::find(satInfo._region);
		assert(!array->empty());

		for (HomeMapEntry const *entry : *array) {
			MemoryPlace const *loc = entry->getHomeNode();
			DataAccessRegion entryRegion = entry->getAccessRegion();
			DataAccessRegion subRegion = satInfo._region.intersect(entryRegion);

			DataAccessRegistration::propagateSatisfiability(
				localTask, subRegion, cpu, hpDependencyData,
				satInfo._readSat, satInfo._writeSat, loc
			);
		}

		delete array;
	}

	void offloadTask(
		Task *task,
		std::vector<SatisfiabilityInfo> const &satInfo,
		ClusterNode const *remoteNode
	) {
		assert(task != nullptr);
		assert(remoteNode != nullptr);

		ClusterNode const *thisNode = ClusterManager::getCurrentClusterNode();
		nanos6_task_info_t *taskInfo = task->getTaskInfo();
		nanos6_task_invocation_info_t *taskInvocationInfo = task->getTaskInvokationInfo();
		size_t flags = task->getFlags();
		void *argsBlock = task->getArgsBlock();
		size_t argsBlockSize = task->getArgsBlockSize();
		size_t nrSatInfo = satInfo.size();
		SatisfiabilityInfo const *satInfoPtr = (nrSatInfo == 0) ? nullptr : satInfo.data();

		MessageTaskNew *msg = new MessageTaskNew(
			thisNode, taskInfo,
			taskInvocationInfo, flags,
			taskInfo->implementation_count, taskInfo->implementations,
			nrSatInfo, satInfoPtr,
			argsBlockSize, argsBlock,
			(void *)task
		);

		ClusterManager::sendMessage(msg, remoteNode);
	}

	void sendRemoteTaskFinished(void *offloadedTaskId, ClusterNode *offloader)
	{
		clusterPrintf("Sending taskfinished for: %p %d\n", offloadedTaskId, offloader->getIndex());
		// Unregister remote tasks first
		assert(offloadedTaskId != nullptr);
		assert(offloader != nullptr);
		RemoteTasksInfoMap::eraseRemoteTaskInfo(offloadedTaskId, offloader->getIndex());

		clusterPrintf("Sending sendRemoteTaskFinished remote task %p %d\n",
			offloadedTaskId, offloader->getIndex());

		// The notify back sending message
		MessageTaskFinished *msg =
			new MessageTaskFinished(ClusterManager::getCurrentClusterNode(), offloadedTaskId);

		ClusterManager::sendMessage(msg, offloader);
	}

	void sendSatisfiability(Task *task, ClusterNode *remoteNode, SatisfiabilityInfo const &satInfo)
	{
		assert(task != nullptr);
		assert(remoteNode != nullptr);
		assert(!satInfo.empty());

		ClusterNode *current = ClusterManager::getCurrentClusterNode();
		MessageSatisfiability *msg = new MessageSatisfiability(current, (void *)task, satInfo);

		ClusterManager::sendMessage(msg, remoteNode);
	}

	void propagateSatisfiabilityForHandler(
		void *offloadedTaskId,
		ClusterNode *offloader,
		SatisfiabilityInfo const &satInfo
	) {
		RemoteTaskInfo &taskInfo
			= RemoteTasksInfoMap::getRemoteTaskInfo(offloadedTaskId, offloader->getIndex());

		taskInfo._lock.lock();
		if (taskInfo._localTask == nullptr) {
			// The remote task has not been created yet, so we
			// just add the info to the temporary vector
			taskInfo._satInfo.push_back(satInfo);
			taskInfo._lock.unlock();
		} else {
			// We *HAVE* to leave the lock now, because propagating
			// satisfiability might lead to unregistering the remote
			// task
			taskInfo._lock.unlock();
			propagateSatisfiability(taskInfo._localTask, satInfo);
		}
	}

	void sendRemoteAccessRelease(
		void *offloadedTaskId,
		ClusterNode const *offloader,
		DataAccessRegion const &region,
		DataAccessType type,
		bool weak,
		MemoryPlace const *location
	) {
		assert(location != nullptr);

		// If location is a host device on this node it is a cluster
		// device from the point of view of the remote node
		if (location->getType() != nanos6_cluster_device) {
			location = ClusterManager::getCurrentMemoryNode();
		}

		ClusterNode *current = ClusterManager::getCurrentClusterNode();

		std::stringstream ss;
		ss << region;

		clusterPrintf("Sending MessageReleaseAccess remote task %p [%s] to %d\n",
			offloadedTaskId,
			ss.str().c_str(),
			offloader->getIndex());

		MessageReleaseAccess *msg =
			new MessageReleaseAccess(current, offloadedTaskId, region, type, weak, location->getIndex());

		ClusterManager::sendMessage(msg, offloader);
	}

	void releaseRemoteAccess(
		Task *task,
		DataAccessRegion const &region,
		DataAccessType type,
		bool weak,
		MemoryPlace const *location
	) {
		assert(task != nullptr);
		assert(location->getType() == nanos6_cluster_device);

		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();

		CPU * const cpu = (currentThread == nullptr) ? nullptr : currentThread->getComputePlace();

		CPUDependencyData localDependencyData;
		CPUDependencyData &hpDependencyData =
			(cpu != nullptr) ? cpu->getDependencyData() : localDependencyData;

		DataAccessRegistration::releaseAccessRegion(
			task, region, type, weak, cpu,
			hpDependencyData, location
		);
	}

	void remoteTaskCreateAndSubmit(
		MessageTaskNew *msg,
		Task *parent,
		bool useCallbackInContext
	) {
		assert(msg != nullptr);
		assert(parent != nullptr);

		nanos6_task_info_t *taskInfo = msg->getTaskInfo();

		size_t numTaskImplementations;
		nanos6_task_implementation_info_t *taskImplementations =
			msg->getImplementations(numTaskImplementations);

		taskInfo->implementations = taskImplementations;
		nanos6_task_invocation_info_t *taskInvocationInfo = msg->getTaskInvocationInfo();

		size_t argsBlockSize;
		void *argsBlock = msg->getArgsBlock(argsBlockSize);

		void *remoteTaskIdentifier = msg->getOffloadedTaskId();
		ClusterNode *remoteNode = ClusterManager::getClusterNode(msg->getSenderId());

		// Create the task with no dependencies. Treat this call
		// as user code since we are inside a spawned task context
		Task *task = AddTask::createTask(
			taskInfo, taskInvocationInfo,
			nullptr, argsBlockSize,
			msg->getFlags(), 0, true
		);
		assert(task != nullptr);

		void *argsBlockPtr = task->getArgsBlock();
		if (argsBlockSize != 0) {
			memcpy(argsBlockPtr, argsBlock, argsBlockSize);
		}

		task->markAsRemote();

		ClusterTaskContext *clusterContext = new TaskOffloading::ClusterTaskContext(
			remoteTaskIdentifier,
			remoteNode
		);
		assert(clusterContext);

		// This is used only in the Namespace
		if (useCallbackInContext) {
			assert(NodeNamespace::isEnabled());

			clusterContext->setCallback(remoteTaskCleanup, msg);
		}

		task->setClusterContext(clusterContext);

		// Register remote Task with TaskOffloading mechanism before
		// submitting it to the dependency system
		RemoteTaskInfo &remoteTaskInfo = RemoteTasksInfoMap::getRemoteTaskInfo(
			remoteTaskIdentifier,
			remoteNode->getIndex()
		);

		std::lock_guard<PaddedSpinLock<>> lock(remoteTaskInfo._lock);
		assert(remoteTaskInfo._localTask == nullptr);
		remoteTaskInfo._localTask = task;

		// Submit the task
		AddTask::submitTask(task, parent, true);

		// Propagate satisfiability embedded in the Message
		size_t numSatInfo;
		TaskOffloading::SatisfiabilityInfo *satInfo = msg->getSatisfiabilityInfo(numSatInfo);
		for (size_t i = 0; i < numSatInfo; ++i) {
			propagateSatisfiability(task, satInfo[i]);
		}

		// Propagate also any satisfiability that has already arrived
		for (SatisfiabilityInfo const &sat : remoteTaskInfo._satInfo) {
			propagateSatisfiability(task, sat);
		}

		remoteTaskInfo._satInfo.clear();
	}

	void remoteTaskWrapper(void *args)
	{
		WorkerThread *workerThread = WorkerThread::getCurrentWorkerThread();
		assert(workerThread != nullptr);

		Task *parent = workerThread->getTask();
		assert(parent != nullptr);

		MessageTaskNew *msg = static_cast<MessageTaskNew *>(args);

		remoteTaskCreateAndSubmit(msg, parent, false);
	}

	void remoteTaskCleanup(void *args)
	{
		assert(args != nullptr);
		MessageTaskNew *msg = static_cast<MessageTaskNew *>(args);

		void *offloadedTaskId = msg->getOffloadedTaskId();
		ClusterNode *offloader = ClusterManager::getClusterNode(msg->getSenderId());

		sendRemoteTaskFinished(offloadedTaskId, offloader);

		// For the moment, we do not delete the Message since it includes the
		// buffers that hold the nanos6_task_info_t and the
		// nanos6_task_implementation_info_t which we might need later on,
		// e.g. Extrae is using these during shutdown. This will change once
		// mercurium gives us access to the respective fields within the
		// binary.
		// delete msg;
	}
}
