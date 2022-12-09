/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/


#ifndef OFFLOADED_TASKS_MAP_H
#define OFFLOADED_TASKS_MAP_H

#include "lowlevel/PaddedSpinLock.hpp"

#include <SatisfiabilityInfo.hpp>

class Task;
class ClusterNode;
namespace TaskOffloading {

	struct OffloadedTaskInfo {
		Task *_origTask;
		const ClusterNode *remoteNode;

		// OffloadedTaskInfo(Task *origTask, Clusternode *remoteNode) : _origTask(nullptr), remoteNode(remoteNode)
		// {
		// }

		// ~OffloadedTaskInfo()
		// {
		// }
	};


	//! When a ClusterNode offloads a task, it attaches an id that is unique
	//! on the offloading node, so we can create a mapping between an
	//! offloaded Task and the matching remote Task.
	//!
	//! Here, we use this id as an index to a container to retrieve
	//! the local information of the remote task.
	class OffloadedTasksInfoMap {
	public:
		//TODO: I keep this like this, in the future make this an unordered map for better
		//scalability
		typedef std::map<OffloadedTaskIdManager::OffloadedTaskId, OffloadedTaskInfo> remote_map_t;

	private:
		//! The actual map holding the remote tasks' info
		remote_map_t _taskMap;

		//! Lock to protect access to the map
		PaddedSpinLock<> _lock;

		//! This is our map for all the remote tasks, currently on the node
		static OffloadedTasksInfoMap *_singleton;

		OffloadedTaskInfo &_createOffloadedTaskInfo(
			OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId,
			Task *origTask, const ClusterNode *remoteNode
		) {
			// clusterPrintf("Adding remoteTaskInfo %lx\n", offloadedTaskId);
			std::lock_guard<PaddedSpinLock<>> guard(_lock);
			assert (_taskMap.count(offloadedTaskId) == 0);
			return _taskMap[offloadedTaskId] = OffloadedTaskInfo({origTask, remoteNode});
		}

		//! This will return a reference to the OffloadedTaskInfo entry
		//! within this map. If this is the first access to this entry
		//! we will create it and return a reference to the new
		//! OffloadedTaskInfo object
		OffloadedTaskInfo &_getOffloadedTaskInfo(
			OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId
		) {
			// clusterPrintf("Getting remoteTaskInfo %lx\n", offloadedTaskId);
			std::lock_guard<PaddedSpinLock<>> guard(_lock);
			return _taskMap[offloadedTaskId];
		}

		//! This erases a map entry. It assumes that there is already
		//! an entry with the given key
		void _eraseTaskInfo(OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId)
		{
			std::lock_guard<PaddedSpinLock<>> guard(_lock);

			// clusterPrintf("Removing remoteTaskInfo %p %d\n", offloadedTaskId, offloaderId);
			//std::cout << clusterBacktrace() << std::endl;

			remote_map_t::iterator it = _taskMap.find(offloadedTaskId);
			assert(it != _taskMap.end());

			_taskMap.erase(it);
		}

	public:
		OffloadedTasksInfoMap() : _taskMap(), _lock()
		{
		}

		static void init()
		{
			assert(_singleton == nullptr);
			_singleton = new OffloadedTasksInfoMap();
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

		//! This will return a reference to the OffloadedTaskInfo entry
		static OffloadedTaskInfo &createOffloadedTaskInfo(
			OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId,
			Task *origTask,
			const ClusterNode *remoteNode
		) {
			assert(_singleton != nullptr);
			return _singleton->_createOffloadedTaskInfo(offloadedTaskId, origTask, remoteNode);
		}

		//! This will return a reference to the OffloadedTaskInfo entry
		static OffloadedTaskInfo &getOffloadedTaskInfo(
			OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId
		) {
			assert(_singleton != nullptr);
			return _singleton->_getOffloadedTaskInfo(offloadedTaskId);
		}

		//! This erases a map entry. It assumes that there is already
		//! an entry with the given key
		static void eraseOffloadedTaskInfo(OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId)
		{
			assert(_singleton != nullptr);
			_singleton->_eraseTaskInfo(offloadedTaskId);
		}
	};

}

#endif /* OFFLOADED_TASKS_MAP_H */
