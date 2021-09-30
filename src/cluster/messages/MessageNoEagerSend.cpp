/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#include "MessageNoEagerSend.hpp"

#include <ClusterManager.hpp>
#include <MessageDelivery.hpp>
#include <TaskOffloading.hpp>

MessageNoEagerSend::MessageNoEagerSend(const ClusterNode *from,
	DataAccessRegion const &region,
	void *offloadedTaskId)
	: Message(NO_EAGER_SEND, sizeof(NoEagerSendMessageContent), from)
{
	_content = reinterpret_cast<NoEagerSendMessageContent *>(_deliverable->payload);
	_content->_region = region;
	_content->_offloadedTaskId = offloadedTaskId;
}

bool MessageNoEagerSend::handleMessage()
{
	Task *task = reinterpret_cast<Task *>(_content->_offloadedTaskId);
	// clusterCout << "handle no eager send " << _content->_region << " for " << task->getLabel() << "\n";
	TaskOffloading::receivedNoEagerSend(task, _content->_region);

	return true;
}

static const bool __attribute__((unused))_registered_dsend =
	Message::RegisterMSGClass<MessageNoEagerSend>(NO_EAGER_SEND);
