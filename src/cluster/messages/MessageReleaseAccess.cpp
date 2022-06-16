/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "tasks/Task.hpp"
#include "MessageReleaseAccess.hpp"

#include <ClusterManager.hpp>
#include <TaskOffloading.hpp>
#include <ClusterUtil.hpp>
#include "OffloadedTaskId.hpp"
#include "OffloadedTasksInfoMap.hpp"
#include "scheduling/Scheduler.hpp"

MessageReleaseAccess::MessageReleaseAccess(
	OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId,
	bool release,
	ReleaseAccessInfoVector &InfoVector
) :
	Message(
		(release && !InfoVector.empty()) ? RELEASE_ACCESS_AND_FINISH
		: !InfoVector.empty() ? RELEASE_ACCESS
		: TASK_FINISHED,
		sizeof(void*) + 2 * sizeof(size_t) + InfoVector.size() * sizeof(ReleaseAccessInfo))
{
	_content = reinterpret_cast<ReleaseAccessMessageContent *>(_deliverable->payload);

	_content->_offloadedTaskId = offloadedTaskId;
	_content->_ninfos = InfoVector.size();
	_content->_release = (size_t) release;

	size_t index = 0;
	for (ReleaseAccessInfo const &accessinfo : InfoVector) {
		_content->_regionInfoList[index++] = accessinfo;
	}
}

bool MessageReleaseAccess::handleMessage()
{
	OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId = _content->_offloadedTaskId;
	TaskOffloading::OffloadedTaskInfo &taskInfo
		= TaskOffloading::OffloadedTasksInfoMap::getOffloadedTaskInfo(offloadedTaskId);
	assert(taskInfo.remoteNode && taskInfo.remoteNode->getIndex() == getSenderId());
	Task *task = taskInfo._origTask;
	TaskOffloading::releaseRemoteAccessForHandler(
		task,
		_content->_ninfos,
		_content->_regionInfoList
	);

	if (_content->_release == 1) {
		ExecutionWorkflow::Step *step = task->getExecutionStep();
		assert(step != nullptr);
		Instrument::offloadedTaskCompletes(task->getInstrumentationTaskId());

		TaskOffloading::OffloadedTasksInfoMap::eraseOffloadedTaskInfo(offloadedTaskId);
		ClusterNode *remoteNode = ClusterManager::getClusterNode(_deliverable->header.senderId);

		// Bookkeeping for balance scheduler
		remoteNode->incNumOffloadedTasks(-1);
		Scheduler::offloadedTaskFinished(remoteNode);

		task->setExecutionStep(nullptr);
		step->releaseSuccessors();
		delete step;
	}

	return true;
}

static const bool __attribute__((unused))_registered_release_access =
	Message::RegisterMSGClass<MessageReleaseAccess>(RELEASE_ACCESS);

static const bool __attribute__((unused))_registered_task_finished =
	Message::RegisterMSGClass<MessageReleaseAccess>(TASK_FINISHED);

static const bool __attribute__((unused))_registered_release_access_and_finish =
	Message::RegisterMSGClass<MessageReleaseAccess>(RELEASE_ACCESS_AND_FINISH);
