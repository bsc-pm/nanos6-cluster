/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/


#include <deque>

#include "NodeNamespace.hpp"
#include "ClusterManager.hpp"
#include "InstrumentCluster.hpp"

#include "system/BlockingAPI.hpp"
#include "system/ompss/AddTask.hpp"
#include "cluster/ClusterUtil.hpp"
#include "cluster/offloading/TaskOffloading.hpp"


NodeNamespace *NodeNamespace::_singleton = nullptr;
bool NodeNamespace::_bodyHasStarted = false;

NodeNamespace::NodeNamespace(SpawnFunction::function_t mainCallback, void *args)
	: _mustShutdown(false),
	_blockedTask(nullptr),
	_callback(mainCallback, args),
	_invocationInfo({"Spawned as a NodeNamespace"})
{
	_taskImplementationInfo.run = NodeNamespace::body;
	_taskImplementationInfo.device_type_id = nanos6_device_t::nanos6_host_device;
	_taskImplementationInfo.task_label = "Cluster_Namespace";
	_taskImplementationInfo.declaration_source = "Cluster Namespace spawned within the runtime";
	_taskImplementationInfo.get_constraints = nullptr;

	_taskInfo.implementations = &_taskImplementationInfo;
	_taskInfo.implementation_count = 1;
	_taskInfo.num_symbols = 0;
	_taskInfo.destroy_args_block = NodeNamespace::destroyArgsBlock;
	_taskInfo.register_depinfo = nullptr;
	_taskInfo.get_priority = nullptr;

	size_t flags = (1 << Task::preallocated_args_block_flag)
		| (size_t) Task::nanos6_task_runtime_flag_t::nanos6_remote_wrapper_flag
		| (size_t) Task::nanos6_task_runtime_flag_t::nanos6_polling_flag;

	// create the task and pass this as argument.
	_namespaceTask = AddTask::createTask(
		&_taskInfo,
		&_invocationInfo,
		this, sizeof(NodeNamespace), flags,
		0 /* Num dependencies */, false /* from user code */
	);
	assert(_namespaceTask != nullptr);
}

NodeNamespace::~NodeNamespace()
{
	// We need to wait until the callback is executed.
	assert(_callback.getCounterValue() == 0);
}


void NodeNamespace::bodyPrivate()
{
	// This is the task body start so this is inside a task and it is not
	// supposed to change during the execution so we register that only
	// once.
	_callback.increment();

	Instrument::stateNodeNamespace(1);

#ifndef NDEBUG
	WorkerThread *workerThread = WorkerThread::getCurrentWorkerThread();
	assert(workerThread != nullptr);
	assert(_namespaceTask != nullptr);
	assert(workerThread->getTask() != nullptr);
	assert(workerThread->getTask() == _namespaceTask);
#endif

	_bodyHasStarted = true;

	while (true) {
		_spinlock.lock();

		if (!_queue.empty()) {
			// Get the first function in the stream's queue
			MessageTaskNew *msg = _queue.front();
			assert(msg != nullptr);
			_queue.pop_front();
			_spinlock.unlock();

			TaskOffloading::remoteTaskCreateAndSubmit(msg, _namespaceTask, true);

		} else {

			if (_mustShutdown.load()) {
				// If we receive the shutdown indication we still need to repeat loops to offload
				// all the pending tasks. _queue.empty requires to be in the lock;
				_spinlock.unlock();
				break;
			}
			// Release the lock and block the task
			_spinlock.unlock();

			_blockedTask.store(_namespaceTask);
			BlockingAPI::blockCurrentTask(false);
			Instrument::stateNodeNamespace(2);
		}
	}

	/* Don't decrement the callback here. The callback is actually decremented
	 * in TaskFinalization::disposeTask when the namespace task is actually
	 * destroyed.  Otherwise it is possible for the NodeNamespace to be
	 * deallocated before the Task itself is destroyed. Later,
	 * TaskFinalization::taskFinished will need to call Task::hasFinished, and
	 * this needs the taskImplementationInfo, which is part of the
	 * NodeNamespace singleton.
	 */
	// _callback.decrement();
}

void NodeNamespace::callbackDecrementPrivate()
{
	int countdown = _callback.decrement();
	assert (countdown >= 0);
	if (countdown == 0) {
		// clusterPrintf("Decremented reached zero\n");
		Instrument::stateNodeNamespace(0);
	}
	// clusterPrintf("Decremented reached something else\n");
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

