/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#include "MessageAccessInfo.hpp"

#include <ClusterManager.hpp>
#include <PendingQueue.hpp>
#include <TaskOffloading.hpp>
#include "OffloadedTaskId.hpp"
#include "OffloadedTasksInfoMap.hpp"

MessageAccessInfo::MessageAccessInfo(
	size_t numRegions,
	const std::vector<TaskOffloading::AccessInfo> &regions)
	: Message(ACCESS_INFO,
		sizeof(OffloadedTaskIdManager::OffloadedTaskId)
		+ sizeof(size_t)
		+ numRegions * sizeof(TaskOffloading::AccessInfo))
{
	_content = reinterpret_cast<AccessInfoMessageContent *>(_deliverable->payload);
	_content->_numRegions = numRegions;
	size_t index = 0;
	for (TaskOffloading::AccessInfo regionInfo : regions) {
		_content->_accessInfo[index] = regionInfo;
		if (index > 0) {
			assert(regionInfo._offloadedTaskId == _content->_offloadedTaskId);
		} else {
			_content->_offloadedTaskId = regionInfo._offloadedTaskId;
		}
		index++;
	}
}

bool MessageAccessInfo::handleMessage()
{
	// clusterCout << "handle access info " << _content->_region << " for " << task->getLabel() << "\n";
	const size_t numRegions = _content->_numRegions;
	for(size_t i = 0; i < numRegions; i++) {
		TaskOffloading::AccessInfo const &regionInfo = _content->_accessInfo[i];
		TaskOffloading::OffloadedTaskInfo &taskInfo = TaskOffloading::OffloadedTasksInfoMap::getOffloadedTaskInfo(regionInfo._offloadedTaskId);
		assert(taskInfo.remoteNode && taskInfo.remoteNode->getIndex() == getSenderId());
		Task *task = taskInfo._origTask;
		TaskOffloading::receivedAccessInfo(task, regionInfo._region);
	}

	return true;
}

static const bool __attribute__((unused))_registered_dsend =
	Message::RegisterMSGClass<MessageAccessInfo>(ACCESS_INFO);
