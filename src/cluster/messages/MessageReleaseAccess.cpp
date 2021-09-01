/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "tasks/Task.hpp"
#include "MessageReleaseAccess.hpp"

#include <ClusterManager.hpp>
#include <TaskOffloading.hpp>
#include <ClusterUtil.hpp>

MessageReleaseAccess::MessageReleaseAccess(
	const ClusterNode *from,
	void *offloadedTaskId,
	bool release,
	ReleaseAccessInfoVector &InfoVector
) :
	Message(
		(release && !InfoVector.empty()) ? RELEASE_ACCESS_AND_FINISH
		: !InfoVector.empty() ? RELEASE_ACCESS
		: TASK_FINISHED,
		sizeof(void*) + 2 * sizeof(size_t) + InfoVector.size() * sizeof(ReleaseAccessInfo), from)
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
	TaskOffloading::releaseRemoteAccessForHandler(
		(Task *)_content->_offloadedTaskId,
		_content->_ninfos,
		_content->_regionInfoList
	);

	if (_content->_release == 1) {
		Task *task = (Task *)_content->_offloadedTaskId;
		ExecutionWorkflow::Step *step = task->getExecutionStep();
		assert(step != nullptr);
		Instrument::offloadedTaskCompletes(task->getInstrumentationTaskId());

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
