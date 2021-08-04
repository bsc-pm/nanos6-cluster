/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_TASK_CONTEXT_HPP
#define CLUSTER_TASK_CONTEXT_HPP

#include <mutex>
#include <vector>

#include <DataAccessRegion.hpp>
#include <MessageTaskNew.hpp>
#include <ClusterManager.hpp>
#include <TaskOffloading.hpp>
#include <ClusterShutdownCallback.hpp>
#include <tasks/Task.hpp>

class ClusterNode;

namespace TaskOffloading {

	//! \brief This class describes the remote context of a task.
	//!
	//! Remote context in this case is the minimum context necessary in
	//! order to be able to identify the counterpart of a Task, on a
	//! remote node. This task can be either an offloaded task, in which
	//! case the ClusterTaskContext object describes the remote task, or it
	//! can be a remote task, so the ClusterTaskContext describes the
	//! offloaded task on the original node.
	//!
	//! The remoteTaskIdentifier is an opaque descriptor that identifies
	//! uniquely, to the user of the ClusterTaskContext object, the remote
	//! task on the remote node. It is opaque so that the user can define
	//! whatever makes sense as a descriptor on each case.
	class ClusterTaskContext {

		MessageTaskNew *_msg;
		//! A descriptro that identifies the remote task at the remote
		//! node
		void *_remoteTaskIdentifier;

		//! The cluster node on which the remote task is located
		ClusterNode *_remoteNode;

		Task *_owner;
		ClusterTaskCallback *_hook;

	public:
		//! \brief Create a Cluster Task context
		//!
		//! \param[in] remoteTaskIdentifier is an identifier of the task
		//!		on the remote node
		//! \param[in] remoteNode is the ClusterNode where the remote
		//!		task is located
		ClusterTaskContext(MessageTaskNew *msg, Task *owner)
			: _msg(msg),
			_remoteTaskIdentifier(msg->getOffloadedTaskId()),
			_remoteNode(ClusterManager::getClusterNode(_msg->getSenderId())),
			_owner(owner),
			_hook(nullptr)
		{
			assert(msg != nullptr);
			assert(owner != nullptr);
		}

		ClusterTaskContext(Task *remoteTaskIdentifier, ClusterNode *remoteNode)
			: _msg(nullptr),
			_remoteTaskIdentifier((void *)remoteTaskIdentifier),
			_remoteNode(remoteNode),
			_owner(remoteTaskIdentifier),
			_hook(nullptr)
		{
		}

		~ClusterTaskContext()
		{

			if (_hook != nullptr) {
				_hook->execute();

				delete _hook;
			}

			if (_msg != nullptr) {
				delete _msg;
			}
		}


		//! \brief Get the remote task descriptor. A descriptro that identifies the remote task at
		//! the remote node
		inline void *getRemoteIdentifier() const
		{
			return _remoteTaskIdentifier;
		}

		//! \brief Get the ClusterNode of the remote task.
		//! The cluster node on which the remote task is located
		inline ClusterNode *getRemoteNode() const
		{
			return _remoteNode;
		}

		inline Task *getOwnerTask() const
		{
			return _owner;
		}

		inline void setCallback(
			SpawnFunction::function_t callback,
			TaskOffloading::ClusterTaskContext *callbackArgs
		) {
			assert(callback != nullptr);
			assert(callbackArgs != nullptr);

			_hook = new ClusterTaskCallback(callback, callbackArgs);
			assert(_hook != nullptr);
		}

	};
}

#endif /* CLUSTER_TASK_CONTEXT_HPP */
