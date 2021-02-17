/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "tasks/Task.hpp"
#include "MessageReleaseAccess.hpp"

#include <ClusterManager.hpp>
#include <TaskOffloading.hpp>
#include <ClusterUtil.hpp>

MessageReleaseAccess::MessageReleaseAccess(const ClusterNode *from,
		void *offloadedTaskId, DataAccessRegion const &region,
		WriteID writeID, int location)
	: Message(RELEASE_ACCESS, sizeof(ReleaseAccessMessageContent), from)
{
	_content = reinterpret_cast<ReleaseAccessMessageContent *>(_deliverable->payload);
	_content->_offloadedTaskId = offloadedTaskId;
	_content->_region = region;
	_content->_writeID = writeID;
	_content->_location = location;
}

bool MessageReleaseAccess::handleMessage()
{
	ClusterMemoryNode *memoryPlace = ClusterManager::getMemoryNode(_content->_location);

	TaskOffloading::releaseRemoteAccess((Task *)_content->_offloadedTaskId,
			_content->_region, _content->_writeID, memoryPlace);

	return true;
}

static const bool __attribute__((unused))_registered_release_access =
	Message::RegisterMSGClass<MessageReleaseAccess>(RELEASE_ACCESS);
