/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/


#ifndef NODENAMESPACE_H
#define NODENAMESPACE_H

#include <deque>

#include "ClusterManager.hpp"
#include "messages/MessageTaskNew.hpp"


class NodeNamespace {
private:
	// This may only be constructed throw the static members.
	NodeNamespace(void (*func)(void *), void *args);

	~NodeNamespace();

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

	void bodyPrivate();

	void callbackPrivate();

	bool tryWakeUp();

	//! \brief Add a function to this executor's stream queue
	//! \param[in] function The kernel to execute
	void enqueueTaskMessagePrivate(MessageTaskNew *message);

	void shutdownPrivate();


public:

	static void body(void *args)
	{
		assert(_singleton != nullptr);
		assert(_singleton == args);

		(void) args;
		_singleton->bodyPrivate();
	}

	static void callback(void *args)
	{
		assert(_singleton != nullptr);
		assert(_singleton == args);

		(void) args;

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
