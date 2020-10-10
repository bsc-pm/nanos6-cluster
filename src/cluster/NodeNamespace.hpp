/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2017 Barcelona Supercomputing Center (BSC)
*/


#ifndef NODENAMESPACE_H
#define NODENAMESPACE_H

#include <deque>

#include "messages/MessageTaskNew.hpp"

class NodeNamespace {
private:

	//! The blocking context of the executor task
	void *_blockingContext;

	//! Whether the runtime is shutting down
	std::atomic<bool> _mustShutdown;

	//! A spinlock to access the queue and block/unblock the executor
	SpinLock _spinlock;

	//! The executor's function queue
	std::deque<MessageTaskNew *> _queue;

	//! ThisTask
	Task *thisTask;

	// The static member
	static NodeNamespace *_singleton;

	static bool isEnabled()
	{
		return (_singleton != nullptr);
	}

	static void remoteTaskCleanup(void *args)
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

	void remoteTaskWrapper(MessageTaskNew *msg)
	{
		assert(args != nullptr);
		MessageTaskNew *msg = static_cast<MessageTaskNew *>(args);

		nanos6_task_info_t *taskInfo = msg->getTaskInfo();

		size_t numTaskImplementations;
		nanos6_task_implementation_info_t *taskImplementations =
			msg->getImplementations(numTaskImplementations);

		taskInfo->implementations = taskImplementations;
		nanos6_task_invocation_info_t *taskInvocationInfo = msg->getTaskInvocationInfo();

		size_t argsBlockSize;
		void *argsBlock = msg->getArgsBlock(argsBlockSize);

		// Remote Task

		// Register remote Task with TaskOffloading mechanism before
		// submitting it to the dependency system
		RemoteTaskInfo &remoteTaskInfo = msg->getRemoteTaskInfo();
		remoteTaskInfo.taskInfoWraper = new nanos6_task_info_t();
		memcpy(remoteTaskInfo.taskInfoWraper, taskInfo, sizeof(nanos6_task_info_t));

		remoteTaskInfo.taskInfoWraper.destroy_args_block = remoteTaskCleanup;
		remoteTaskInfo.taskInfoWraper.task_type_data = taskInfo;    // Wrapper pointer to next



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
		task->setClusterContext(msg->allocateClusterTaskContext());


		std::lock_guard<PaddedSpinLock<>> lock(remoteTaskInfo._lock);
		assert(remoteTaskInfo._localTask == nullptr);
		remoteTaskInfo._localTask = task;

		WorkerThread *workerThread = WorkerThread::getCurrentWorkerThread();
		assert(workerThread != nullptr);

		Task *parent = workerThread->getTask();
		assert(parent != nullptr);

		// Submit the task
		AddTask::submitTask(task, parent, true);

		// Propagate satisfiability embedded in the Message
		size_t numSatInfo;
		TaskOffloading::SatisfiabilityInfo *satInfo = msg->getSatisfiabilityInfo(numSatInfo);
		for (size_t i = 0; i < numSatInfo; ++i) {
			propagateSatisfiability(task, satInfo[i]);
		}

		// Propagate also any satisfiability that has already arrived
		if (!remoteTaskInfo._satInfo.empty()) {
			propagateSatisfiability(task, remoteTaskInfo._satInfo);
			remoteTaskInfo._satInfo.clear();
		}
	}

	//! \brief Add a function to this executor's stream queue
	//! \param[in] function The kernel to execute
	static void enqueueTaskMessage(MessageTaskNew *message)
	{
		_singleton->_spinlock.lock();

		_singleton->_queue.push_back(message);
		void *blockingContext = _singleton->_blockingContext;
		_blockingContext = nullptr;

		_spinlock.unlock();

		// Unblock the executor if it was blocked
		if (blockingContext != nullptr) {
			nanos6_unblock_task(blockingContext);
		}
	}

	static void body(void *args)
	{
		assert(_singleton != nullptr);

		// This is the task body start so this is inside a task and it is not supposed to change
		// during the execution so we register that only once.
		WorkerThread *workerThread = WorkerThread::getCurrentWorkerThread();
		assert(workerThread != nullptr);

		_singleton->thisTask = workerThread->getTask();
		assert(_singleton->thisTask != nullptr);

		while (!_mustShutdown.load()) {
			_spinlock.lock();

			if (!_queue.empty()) {
				// Get the first function in the stream's queue
				MessageTaskNew *msg = _queue.front();
				assert(msg != nullptr);
				_queue.pop_front();

				_spinlock.unlock();

				RemoteTaskInfo *remoteTaskInfo = nullptr;
				Task *task = taskFromMessage(msg, &remoteTaskInfo);
				assert(remoteTaskInfo != nullptr);

				//! Task Info copy and reassign
				nanos6_task_info_t *originalTaskInfo = task->getTaskInfo();
				remoteTaskInfo->_taskInfoWraper = *originalTaskInfo;


				
				remoteTaskInfo->_taskInfoWraper->task_type_data = originalTaskInfo;



			} else {
				// Release the lock and block the task
				assert(_blockingContext == nullptr);
				_blockingContext = nanos6_get_current_blocking_context();
				void *blockingContext = _blockingContext;

				_spinlock.unlock();

				nanos6_block_current_task(blockingContext);
			}
		}

	}


protected:

	NodeNamespace()
	{
	}

	inline ~NodeNamespace()
	{

	}

	// All static functions here
	static init()
	{
		SpawnFunction::spawnFunction(
			NodeNamespace::body,
			nullptr,
			nullptr,
			nullptr,
			label.c_str(),
			false,
			(size_t) Task::nanos6_task_runtime_flag_t::nanos6_polling_flag
		);
	}

};


#endif /* NODENAMESPACE_H */
