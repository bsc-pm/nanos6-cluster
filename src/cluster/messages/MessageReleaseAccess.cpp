/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "MessageReleaseAccess.hpp"

#include <ClusterManager.hpp>
#include <TaskOffloading.hpp>

MessageReleaseAccess::MessageReleaseAccess(const ClusterNode *from,
		void *offloadedTaskId, DataAccessRegion const &region,
		DataAccessType type, bool weak, WriteID writeID, int location)
	: Message(RELEASE_ACCESS, sizeof(ReleaseAccessMessageContent), from)
{
	_content = reinterpret_cast<ReleaseAccessMessageContent *>(_deliverable->payload);
	_content->_offloadedTaskId = offloadedTaskId;
	_content->_region = region;
	_content->_type = type;
	_content->_weak = weak;
	_content->_writeID = writeID;
	_content->_location = location;
}

bool MessageReleaseAccess::handleMessage()
{
	ClusterMemoryNode *memoryPlace =
		ClusterManager::getMemoryNode(_content->_location);

	std::stringstream ss;
	ss << _content->_region;

	printf("Node %d: Handle MessageReleaseAccess task %p [%s]\n",
		ClusterManager::getCurrentClusterNode()->getIndex(),
		_content->_offloadedTaskId,
		ss.str().c_str()
	);

	TaskOffloading::releaseRemoteAccess((Task *)_content->_offloadedTaskId,
			_content->_region, _content->_type, _content->_weak, _content->_writeID, memoryPlace);

	return true;
}

//! Register the Message type to the Object factory
static Message *createReleaseAccessMessage(Message::Deliverable *dlv)
{
	return new MessageReleaseAccess(dlv);
}

static const bool __attribute__((unused))_registered_release_access =
	REGISTER_MSG_CLASS(RELEASE_ACCESS, createReleaseAccessMessage);
