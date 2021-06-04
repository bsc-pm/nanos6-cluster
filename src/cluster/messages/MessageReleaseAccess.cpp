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
	ReleaseAccessInfoVector &InfoVector
) :
	Message(RELEASE_ACCESS,
		sizeof(void*) + sizeof(size_t) + InfoVector.size() * sizeof(ReleaseAccessInfo), from)
{
	_content = reinterpret_cast<ReleaseAccessMessageContent *>(_deliverable->payload);

	_content->_offloadedTaskId = offloadedTaskId;
	_content->_ninfos = InfoVector.size();

	size_t index = 0;
	for (ReleaseAccessInfo const &accessinfo : InfoVector) {
		_content->_regionInfoList[index++] = accessinfo;
	}
}

bool MessageReleaseAccess::handleMessage()
{
	TaskOffloading::releaseRemoteAccess(
		(Task *)_content->_offloadedTaskId,
		_content->_ninfos,
		_content->_regionInfoList
	);

	return true;
}

static const bool __attribute__((unused))_registered_release_access =
	Message::RegisterMSGClass<MessageReleaseAccess>(RELEASE_ACCESS);
