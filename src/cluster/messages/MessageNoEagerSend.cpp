/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#include "MessageNoEagerSend.hpp"

#include <ClusterManager.hpp>
#include <MessageDelivery.hpp>
#include <TaskOffloading.hpp>

MessageNoEagerSend::MessageNoEagerSend(const ClusterNode *from,
	size_t numRegions,
	const std::vector<DataAccessRegion> &regions,
	void *offloadedTaskId)
	: Message(NO_EAGER_SEND, sizeof(size_t) + numRegions * sizeof(NoEagerSendRegion), from)
{
	_content = reinterpret_cast<NoEagerSendMessageContent *>(_deliverable->payload);
	_content->_numRegions = numRegions;
	size_t index = 0;
	for (DataAccessRegion region : regions) {
		_content->_noEagerSendInfo[index]._region = region;
		_content->_noEagerSendInfo[index]._offloadedTaskId = offloadedTaskId;
		index++;
	}
}

bool MessageNoEagerSend::handleMessage()
{
	// clusterCout << "handle no eager send " << _content->_region << " for " << task->getLabel() << "\n";
	const size_t numRegions = _content->_numRegions;
	for(size_t i = 0; i < numRegions; i++) {
		NoEagerSendRegion const &regionInfo = _content->_noEagerSendInfo[i];
		Task *task = reinterpret_cast<Task *>(regionInfo._offloadedTaskId);
		TaskOffloading::receivedNoEagerSend(task, regionInfo._region);
	}

	return true;
}

static const bool __attribute__((unused))_registered_dsend =
	Message::RegisterMSGClass<MessageNoEagerSend>(NO_EAGER_SEND);
