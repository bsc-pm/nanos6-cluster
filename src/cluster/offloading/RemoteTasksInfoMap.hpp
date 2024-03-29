/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/


#ifndef REMOTE_TASKS_MAP_H
#define REMOTE_TASKS_MAP_H

#include "lowlevel/PaddedSpinLock.hpp"

#include <SatisfiabilityInfo.hpp>

#include "OffloadedTaskId.hpp"

namespace TaskOffloading {

	//! Information for tasks that have been offloaded to the
	//! current node. Objects of this type are used to temporarily
	//! keep satisfiability info the might arrive ahead of the
	//! creation of the actual task.
	struct RemoteTaskInfo {
		Task *_localTask;
		std::vector<SatisfiabilityInfo> _satInfo;
		PaddedSpinLock<> _lock;

		RemoteTaskInfo() : _localTask(nullptr), _satInfo(), _lock()
		{
		}

		~RemoteTaskInfo()
		{
		}
	};


	//! When a ClusterNode offloads a task, it attaches an id that is unique
	//! on the offloading node, so we can create a mapping between an
	//! offloaded Task and the matching remote Task.
	//!
	//! Here, we use this id as an index to a container to retrieve
	//! the local information of the remote task.
	class RemoteTasksInfoMap {
	public:
		//TODO: I keep this like this, in the future make this an unordered map for better
		//scalability
		typedef std::map<OffloadedTaskIdManager::OffloadedTaskId, RemoteTaskInfo> remote_map_t;

	private:
		//! The actual map holding the remote tasks' info
		remote_map_t _taskMap;

		//! Lock to protect access to the map
		PaddedSpinLock<> _lock;

		//! This is our map for all the remote tasks, currently on the node
		static RemoteTasksInfoMap *_singleton;


		//! This will return a reference to the RemoteTaskInfo entry
		//! within this map. If this is the first access to this entry
		//! we will create it and return a reference to the new
		//! RemoteTaskInfo object
		RemoteTaskInfo &_getRemoteTaskInfo(OffloadedTaskIdManager::OffloadedTaskId offloadTaskId)
		{
			// clusterPrintf("Adding/getting remoteTaskInfo %lx\n", offloadTaskId);
			std::lock_guard<PaddedSpinLock<>> guard(_lock);
			return _taskMap[offloadTaskId];
		}

		//! This erases a map entry. It assumes that there is already
		//! an entry with the given key
		void _eraseTaskInfo(OffloadedTaskIdManager::OffloadedTaskId offloadTaskId)
		{
			std::lock_guard<PaddedSpinLock<>> guard(_lock);

			// clusterPrintf("Removing remoteTaskInfo %lx\n", offloadTaskId);
			//std::cout << clusterBacktrace() << std::endl;

			remote_map_t::iterator it = _taskMap.find(offloadTaskId);
			assert(it != _taskMap.end());

			_taskMap.erase(it);
		}

	public:
		RemoteTasksInfoMap() : _taskMap(), _lock()
		{
		}

		static void init()
		{
			assert(_singleton == nullptr);
			_singleton = new RemoteTasksInfoMap();
			assert(_singleton != nullptr);
		}

		static void shutdown()
		{
			// TODO: Assert that the map is empty before deleting this.
			assert(_singleton != nullptr);

			// There is a memory leak somewhere
			// assert(_singleton->_taskMap.empty());
			delete _singleton;
			_singleton = nullptr;
		}

		//! This will return a reference to the RemoteTaskInfo entry
		//! within this map. If this is the first access to this entry
		//! we will create it and return a reference to the new
		//! RemoteTaskInfo object
		static RemoteTaskInfo &getRemoteTaskInfo(OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId)
		{
			assert(_singleton != nullptr);
			return _singleton->_getRemoteTaskInfo(offloadedTaskId);
		}

		//! This erases a map entry. It assumes that there is already
		//! an entry with the given key
		static void eraseRemoteTaskInfo(OffloadedTaskIdManager::OffloadedTaskId offloadTaskId)
		{
			assert(_singleton != nullptr);
			_singleton->_eraseTaskInfo(offloadTaskId);
		}
	};

}

#endif /* REMOTE_TASKS_MAP_H */
