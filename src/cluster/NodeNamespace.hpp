/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/


#ifndef NODENAMESPACE_H
#define NODENAMESPACE_H

#include <deque>

#include "ClusterManager.hpp"
#include "messages/MessageTaskNew.hpp"
#include "tasks/Task.hpp"


class NodeNamespace {
private:
	// This may only be constructed throw the static members.
	NodeNamespace(SpawnFunction::function_t mainCallback, void *args);

	~NodeNamespace();

	//! Whether the runtime is shutting down
	std::atomic<bool> _mustShutdown;

	//! A spinlock to access the queue and block/unblock the executor
	SpinLock _spinlock;

	//! The executor's function queue
	std::deque<MessageTaskNew *> _queue;

	// Pointers to this task
	std::atomic<Task *> _blockedTask;  // This will be used to know if the task
									   // is actually blocked to restart it
	// This is the callback needed in the main wrapper.
	ClusterShutdownCallback _callback;

	// This is needed to spawn the task manually.
	nanos6_task_invocation_info_t _invocationInfo;
	nanos6_task_info_t _taskInfo;
	nanos6_task_implementation_info_t _taskImplementationInfo;
	Task *_namespaceTask;

	// this is the singleton... Nos strictly needed to do it in this way.
	static NodeNamespace *_singleton;

	void bodyPrivate();

	void callbackDecrementPrivate();

	bool tryWakeUp();

	//! \brief Add a function to this executor's stream queue
	//! \param[in] function The kernel to execute
	void enqueueTaskMessagePrivate(MessageTaskNew *message);

public:

	static void body(void *args, void *, nanos6_address_translation_entry_t *)
	{
		assert(_singleton != nullptr);
		assert(_singleton == args);

		NodeNamespace *ptr = static_cast<NodeNamespace *>(args);
		assert(ptr != nullptr);
		(void)ptr;

		_singleton->bodyPrivate();
	}

	static void init(void (*func)(void *), void *args)
	{
		assert(ClusterManager::inClusterMode() == true);
		assert(ClusterManager::isMasterNode() == false);
		assert(_singleton == nullptr);

		_singleton = new NodeNamespace(func, args);
		assert(_singleton != nullptr);
	}

	static void callbackIncrement()
	{
		assert(_singleton != nullptr);
		// First assert that the loop already started....
		// There is not a clear way to force that. Let's hope for now.
		assert(_singleton->_callback.getCounterValue() > 0);

		_singleton->_callback.increment();
	}

	static void callbackDecrement()
	{
		assert(_singleton != nullptr);
		_singleton->callbackDecrementPrivate();
	}

	static void notifyShutdown()
	{
		assert(_singleton != nullptr);
		assert(_singleton->_mustShutdown.load() == false);

		// The loop checks two conditions to exit.
		_singleton->_mustShutdown.store(true);
		_singleton->tryWakeUp();
	}

	static void deallocate()
	{
		assert(_singleton->_mustShutdown.load() == true);
		assert(_singleton->_callback.getCounterValue() == 0);

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
