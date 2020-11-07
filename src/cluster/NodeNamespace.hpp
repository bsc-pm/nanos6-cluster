/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2017 Barcelona Supercomputing Center (BSC)
*/


#ifndef NODENAMESPACE_H
#define NODENAMESPACE_H

#include <deque>

#include "ClusterManager.hpp"

#include "messages/MessageTaskNew.hpp"
#include "system/BlockingAPI.hpp"

class NodeNamespace {
private:

	//! Whether the runtime is shutting down
	std::atomic<bool> _mustShutdown;

	//! A spinlock to access the queue and block/unblock the executor
	SpinLock _spinlock;

	//! The executor's function queue
	std::deque<MessageTaskNew *> _queue;

	// Pointers to this task
	Task *_namespaceTask;
	std::atomic<Task *> _blockedTask;  // This will be used to know if the task
									   // is actually blocked to restart it
	// This is the callback needed in the main wrapper.
	ClusterShutdownCallback *_callback;

	// Shutdown condition var. The current callback actually overlap this functionality, but I refer
	// independent variables for independent functionalities.
	ConditionVariable _condVar;

	// this is the singleton... Nos strictly needed to do it in this way.
	static NodeNamespace *_singleton;

	void bodyPrivate()
	{
		// This is the task body start so this is inside a task and it is not
		// supposed to change during the execution so we register that only
		// once.
		WorkerThread *workerThread = WorkerThread::getCurrentWorkerThread();
		assert(workerThread != nullptr);

		_namespaceTask = workerThread->getTask();
		assert(_namespaceTask != nullptr);

		while (true) {
			_spinlock.lock();
			// If we receive the shutdown indication we still need to repeat loops to offload all
			// the pending tasks. _queue.empty requires to be in the lock;
			if (_mustShutdown.load() && _queue.empty()) {
				_spinlock.unlock();
				break;
			}

			if (!_queue.empty()) {
				// Get the first function in the stream's queue
				MessageTaskNew *msg = _queue.front();
				assert(msg != nullptr);
				_queue.pop_front();
				_spinlock.unlock();

				TaskOffloading::remoteTaskCreateAndSubmit(msg, _namespaceTask, true);

			} else {
				// Release the lock and block the task
				_spinlock.unlock();

				_blockedTask.store(_namespaceTask);
				BlockingAPI::blockCurrentTask(false);;
			}
		}
	}

	void callbackPrivate()
	{
		assert(_callback != nullptr);
		_callback->execute();

		_condVar.signal();
	}

	bool tryWakeUp()
	{
		// Unblock the executor if it was blocked
		if (_blockedTask.load() != nullptr) {
			assert(_blockedTask == _namespaceTask);

			_blockedTask.store(nullptr);
			BlockingAPI::unblockTask(_namespaceTask, false);
			return true;
		}
		return false;
	}

	//! \brief Add a function to this executor's stream queue
	//! \param[in] function The kernel to execute
	void enqueueTaskMessagePrivate(MessageTaskNew *message)
	{
		// We shouldn't receive any task-new after Shutdown message.
		assert(!_mustShutdown.load());

		_spinlock.lock();

		_queue.push_back(message);

		_spinlock.unlock();

		// Unblock the executor if it was blocked
		tryWakeUp();
	}

	void shutdownPrivate()
	{
		printf("Called %s\n", __func__);
		assert(_singleton != nullptr);

		_singleton->_mustShutdown.store(true);

		tryWakeUp();
	}

protected:

	NodeNamespace(void (*func)(void *), void *args) :
		_mustShutdown(false),
		_namespaceTask(nullptr),
		_blockedTask(nullptr),
		_callback(new ClusterShutdownCallback(func, args)),
		_condVar()
	{
		assert(_callback != nullptr);

		SpawnFunction::spawnFunction(
			NodeNamespace::body, this,
			NodeNamespace::callback, this,
			"Cluster_Namespace",
			false,
			(size_t) Task::nanos6_task_runtime_flag_t::nanos6_polling_flag
		);
	}

	inline ~NodeNamespace()
	{
		// We need to wait until the callback is executed.
		_condVar.wait();

		delete _callback;
	}

public:

	static void body(void *args)
	{
		assert(_singleton != nullptr);
		assert(_singleton == args);

		_singleton->bodyPrivate();
	}

	static void callback(void *args)
	{
		assert(_singleton != nullptr);
		assert(_singleton == args);

		_singleton->callbackPrivate();
	}

	static void init(void (*func)(void *), void *args)
	{
		assert(ClusterManager::inClusterMode() == true);
		assert(ClusterManager::isMasterNode() == false);
		assert(_singleton == nullptr);

		_singleton = new NodeNamespace(func, args);
		assert(_singleton != nullptr);
	}

	static void shutdown()
	{
		assert(_singleton != nullptr);

		_singleton->shutdownPrivate();
	}

	static void deallocate()
	{
		delete _singleton;
		_singleton = nullptr;
	}

	//! \brief Add a function to this executor's stream queue
	//! \param[in] function The kernel to execute
	static void enqueueTaskMessage(MessageTaskNew *message)
	{
		_singleton->enqueueTaskMessagePrivate(message);
	}

	static bool isEnabled()
	{
		return (_singleton != nullptr);
	}
};


#endif /* NODENAMESPACE_H */
