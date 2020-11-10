/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/


#include <deque>

#include "NodeNamespace.hpp"
#include "ClusterManager.hpp"
#include "InstrumentCluster.hpp"

#include "system/BlockingAPI.hpp"


NodeNamespace *NodeNamespace::_singleton = nullptr;


NodeNamespace::NodeNamespace(void (*func)(void *), void *args) :
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

NodeNamespace::~NodeNamespace()
{
	// We need to wait until the callback is executed.
	printf("Called %s\n", __func__);

	_condVar.wait();

	delete _callback;
}


void NodeNamespace::bodyPrivate()
{
	// This is the task body start so this is inside a task and it is not
	// supposed to change during the execution so we register that only
	// once.
	Instrument::stateNodeNamespace(1);

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
			BlockingAPI::blockCurrentTask(false);
			Instrument::stateNodeNamespace(2);

		}
	}
}

void NodeNamespace::callbackPrivate()
{
	printf("Called %s\n", __func__);
	assert(_callback != nullptr);
	_callback->execute();

	_condVar.signal();
	Instrument::stateNodeNamespace(0);
}

bool NodeNamespace::tryWakeUp()
{
	// Unblock the executor if it was blocked
	if (_blockedTask.load() != nullptr) {
		assert(_blockedTask == _namespaceTask);

		_blockedTask.store(nullptr);
		BlockingAPI::unblockTask(_namespaceTask, false);
		Instrument::stateNodeNamespace(3);

		return true;
	}
	return false;
}

//! \brief Add a function to this executor's stream queue
//! \param[in] function The kernel to execute
void NodeNamespace::enqueueTaskMessagePrivate(MessageTaskNew *message)
{
	// We shouldn't receive any task-new after Shutdown message.
	assert(!_mustShutdown.load());

	_spinlock.lock();

	_queue.push_back(message);

	_spinlock.unlock();

	// Unblock the executor if it was blocked
	tryWakeUp();
}

void NodeNamespace::shutdownPrivate()
{
	printf("Called %s\n", __func__);
	assert(_singleton != nullptr);

	_singleton->_mustShutdown.store(true);

	tryWakeUp();
}
