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

		// Convert integer source id to a pointer to the relevant MemoryPlace
		MemoryPlace const *loc;
		if (satInfo._src == -1) {
			 // -1 means nullptr: see comment in ClusterDataLinkStep::linkRegion(). It
			 // happens for race conditions when write satisfiability is propagated
			 // before read satisfiability.
			loc = nullptr;
		} else	{
			// Otherwise it is either the node index or the directory (which is used
			// for uninitialized memory regions).
			loc = ClusterManager::getMemoryNodeOrDirectory(satInfo._src);
		}

		DataAccessRegistration::propagateSatisfiability(
			localTask, satInfo._region, cpu,
			hpDependencyData, satInfo._readSat,
			satInfo._writeSat, satInfo._writeID, loc
		);
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

		Instrument::taskIsOffloaded(task->getInstrumentationTaskId());
		task->markAsOffloaded();

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
		// clusterPrintf("Sending taskfinished for: %p %d\n", offloadedTaskId, offloader->getIndex());
		// Unregister remote tasks first
		assert(offloadedTaskId != nullptr);
		assert(offloader != nullptr);
		RemoteTasksInfoMap::eraseRemoteTaskInfo(offloadedTaskId, offloader->getIndex());

		// clusterPrintf("Sending sendRemoteTaskFinished remote task %p %d\n",
			// offloadedTaskId, offloader->getIndex());

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
		WriteID writeID,
		MemoryPlace const *location
	) {
		assert(location != nullptr);

		// If location is a host device on this node it is a cluster
		// device from the point of view of the remote node
		if (location->getType() != nanos6_cluster_device
			&& !Directory::isDirectoryMemoryPlace(location)) {
			location = ClusterManager::getCurrentMemoryNode();
		}

		ClusterNode *current = ClusterManager::getCurrentClusterNode();

		std::stringstream ss;
		ss << region;

		// clusterPrintf("Sending MessageReleaseAccess remote task %p [%s] to %d\n",
		// 	offloadedTaskId,
		// 	ss.str().c_str(),
		// 	offloader->getIndex());

		MessageReleaseAccess *msg =
			new MessageReleaseAccess(current, offloadedTaskId, region, writeID, location->getIndex());

		ClusterManager::sendMessage(msg, offloader);
	}

	void releaseRemoteAccess(
		Task *task,
		DataAccessRegion const &region,
		WriteID writeID,
		MemoryPlace const *location
	) {
		assert(task != nullptr);
		assert(Directory::isDirectoryMemoryPlace(location) || location->getType() == nanos6_cluster_device);

		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();

		CPU * const cpu = (currentThread == nullptr) ? nullptr : currentThread->getComputePlace();

		CPUDependencyData localDependencyData;
		CPUDependencyData &hpDependencyData =
			(cpu != nullptr) ? cpu->getDependencyData() : localDependencyData;

		DataAccessRegistration::releaseAccessRegion(
			task, region,
			/* not relevant as specifyingDependency = false */ NO_ACCESS_TYPE,
			/* not relevant as specifyingDependency = false */ false,
			cpu,
			hpDependencyData, writeID, location, /* specifyingDependency */ false
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

		// Check satisfiability for noRemotePropagation
		size_t numSatInfo;
		TaskOffloading::SatisfiabilityInfo *satInfo = msg->getSatisfiabilityInfo(numSatInfo);
		for (size_t i = 0; i < numSatInfo; ++i) {
			DataAccessRegistration::setNamespacePredecessor(task, parent, satInfo[i]._region, remoteNode, satInfo[i]._namespacePredecessor);
		}

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
		// assert(remoteTaskInfo._localTask == nullptr);
		remoteTaskInfo._localTask = task;

		// TODO: This is a workaround for the case where the task actually
		// executes and finishes before we leave this function (and release
		// remoteTaskInfo._lock). When this happens the remoteTaskInfo is
		// actually destroyed inside the same thread (i.e. as a nested function
		// call inside AddTask::submitTask or propagateSatisfiability). This
		// raises an assertion in the debug build (lock destroyed while held by
		// the same thread) and would be potential memory corruption otherwise.
		// This is a workaround that stops this happening, but it does cause
		// a memory leak on the remoteTaskInfos.
		remoteTaskInfo._taskBeingConstructed = true;

		// Submit the task
		AddTask::submitTask(task, parent, true);

		// If the task does not have the wait flag then, unless
		// cluster.disable_autowait=false, set the "autowait" flag, which will
		// enable early release for accesses ("locally") propagated in the namespace
		// and use delayed release for the ("non-local") accesses that require a
		// message back to the offloader.
		if (!task->mustDelayRelease()) {
			if (!ClusterManager::getDisableAutowait()) {
				task->setDelayedNonLocalRelease();
			}
		}

		// Propagate satisfiability embedded in the Message
		for (size_t i = 0; i < numSatInfo; ++i) {
			if (satInfo[i]._readSat || satInfo[i]._writeSat) {
				propagateSatisfiability(task, satInfo[i]);
			}
		}

		// Propagate also any satisfiability that has already arrived
		for (SatisfiabilityInfo const &sat : remoteTaskInfo._satInfo) {
			propagateSatisfiability(task, sat);
		}

		remoteTaskInfo._taskBeingConstructed = false;
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
