/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/


#ifndef NODENAMESPACE_H
#define NODENAMESPACE_H

#include <deque>
#include <unistd.h>

#include <ClusterShutdownCallback.hpp>
#include "system/ompss/AddTask.hpp"
#include "messages/MessageTaskNew.hpp"

class Task;

class NodeNamespace {
private:
	// This may only be constructed throw the static members.
	NodeNamespace(SpawnFunction::function_t mainCallback, void *args);

	~NodeNamespace();

	//! Whether the body has started executing
	std::mutex m;
	bool _bodyHasStarted;
	std::condition_variable cv;

	//! Whether the runtime is shutting down
	std::atomic<bool> _mustShutdown;

	//! A spinlock to access the queue and block/unblock the executor
	SpinLock _spinlock;

	//! The executor's function queue
	std::deque<MessageTaskNew *> _queue;

	//! The actions function
	Message *_messageAction;

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
		assert(_singleton == nullptr);

		_singleton = new NodeNamespace(func, args);
		assert(_singleton != nullptr);

		// Submit the NodeNamespace task after initializing _singleton in the
		// above statement. Otherwise it is possible for the body to begin
		// executing and for NodeNamespace::body to dereference _singleton
		// before it is written.
		AddTask::submitTask(_singleton->_namespaceTask, nullptr, false);
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

	// This hook runs in disposeTask. It is the simplest addition to execute code there before
	// deleting the singleton task.
	static void destroyArgsBlock(void *)
	{
		callbackDecrement();
	}

	static void notifyShutdown()
	{
		assert(_singleton != nullptr);
		assert(!_singleton->_mustShutdown.load());

		// The loop checks two conditions to exit.
		_singleton->_mustShutdown.store(true);
		_singleton->tryWakeUp();
	}

	static void deallocate()
	{
		assert(_singleton->_mustShutdown.load() == true);

		/* Wait until body has started, otherwise a very short program that
		 * doesn't use this node may start shutting down before the
		 * namespace body has started executing. When the body does start
		 * executing it would otherwise find that _singleton is nullptr.
		 */
		{
			std::unique_lock<std::mutex> lk(_singleton->m);
			_singleton->cv.wait(lk, []{return _singleton->_bodyHasStarted;});
		}

		/*
		 * TODO: the shutdown procedure on node 0 should start when both main
		 * and the NodeNamespace have finished. Currently it starts when the
		 * callback from main is called, i.e. once main has finished.  If main
		 * has finished, then the NodeNamespace on node 0 should have finished
		 * executing tasks, but there is a race condition between (a) sending
		 * the completion messages among nodes, ultimately back to node 0 and
		 * finishing main and (b) finalizing the NodeNamespace task on node 0.
		 * This race condition is much more likely when there is more than one
		 * MPI rank (="node" within Nanos6) per physical node. This is a hack
		 * that will serve for now.
		 */
		while (_singleton->_callback.getCounterValue() > 0) {
			// clusterCout << "Waiting for NodeNamespace callback counter to become zero...\n";
			sleep(1);
		}

		delete _singleton;
		_singleton = nullptr;
	}

	//! \brief Add a function to this executor's stream queue
	//! \param[in] function The kernel to execute
	static void enqueueMessagesTaskNew(const std::deque<MessageTaskNew*> &messages)
	{
		assert(!_singleton->_mustShutdown.load());
		assert(!messages.empty());

		{
			std::lock_guard<SpinLock> guard(_singleton->_spinlock);
			_singleton->_queue.insert(_singleton->_queue.end(), messages.begin(), messages.end());
		}
		_singleton->tryWakeUp();
	}

	//! \brief Add a function to this executor's stream queue
	//! \param[in] function The kernel to execute
	static void setActionMessage(Message* message)
	{
		assert(!_singleton->_mustShutdown.load());
		assert(message != nullptr);
		{
			std::lock_guard<SpinLock> guard(_singleton->_spinlock);
			assert(_singleton->_messageAction == nullptr);
			_singleton->_messageAction = message;
		}
		_singleton->tryWakeUp();
	}


	static bool isEnabled()
	{
		return (_singleton != nullptr);
	}

	static Task *getNamespaceTask()
	{
		assert (_singleton != nullptr);
		return _singleton->_namespaceTask;
	}
};


#endif /* NODENAMESPACE_H */
