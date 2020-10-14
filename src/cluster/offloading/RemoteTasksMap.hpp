/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/


#ifndef REMOTETASKS_H
#define REMOTETASKS_H

#include "lowlevel/PaddedSpinLock.hpp"

#include <SatisfiabilityInfo.hpp>

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
			delete taskInfoWraper;
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
		typedef std::pair<void *, int> remote_index_t;
		//TODO: I keeep this like this, in the future make this an unordered map for better
		//scalability
		typedef std::map<remote_index_t, RemoteTaskInfo> remote_map_t;

		RemoteTasksMap() : _taskMap(), _lock()
		{
		}

		static void init()
		{
			assert(_singleton == nullptr);
			_singleton = new RemoteTasks();
			assert(_singleton != nullptr);
		}

		static void shutdown()
		{
			// TODO: Assert that the map is empty before deleting this.
			assert(_singleton != nullptr);
			assert(_singleton->_taskMap.empty());
			delete _singleton;
			_singleton = nullptr;
		}

		//! This will return a reference to the RemoteTaskInfo entry
		//! within this map. If this is the first access to this entry
		//! we will create it and return a reference to the new
		//! RemoteTaskInfo object
		static RemoteTaskInfo &getRemoteTaskInfo(void *offloadedTaskId, int offloaderId)
		{
			assert(_singleton != nullptr);
			return _singleton->_getRemoteTaskInfo(offloadedTaskId, offloaderId);
		}

		//! This erases a map entry. It assumes that there is already
		//! an entry with the given key
		static void eraseRemoteTaskInfo(void *offloadTaskId, int offloaderId)
		{
			assert(_singleton != nullptr);
			_singleton->_eraseTaskInfo(offloadTaskId, offloaderId);
		}

	private:
		//! The actual map holding the remote tasks' info
		remote_map_t _taskMap;

		//! Lock to protect access to the map
		PaddedSpinLock<> _lock;

		//! This is our map for all the remote tasks, currently on the node
		static RemoteTasksMap *_singleton;


		//! This will return a reference to the RemoteTaskInfo entry
		//! within this map. If this is the first access to this entry
		//! we will create it and return a reference to the new
		//! RemoteTaskInfo object
		RemoteTaskInfo &_getRemoteTaskInfo(void *offloadedTaskId, int offloaderId)
		{
			auto key = std::make_pair(offloadedTaskId, offloaderId);

			std::lock_guard<PaddedSpinLock<>> guard(_lock);
			return _taskMap[key];
		}

		//! This erases a map entry. It assumes that there is already
		//! an entry with the given key
		void _eraseTaskInfo(void *offloadTaskId, int offloaderId)
		{
			auto key = std::make_pair(offloadTaskId, offloaderId);

			std::lock_guard<PaddedSpinLock<>> guard(_lock);

			remote_map_t::iterator it = _taskMap.find(key);
			assert(it != _taskMap.end());

			_taskMap.erase(it);
		}
	};

}

#endif /* REMOTETASKS_H */
