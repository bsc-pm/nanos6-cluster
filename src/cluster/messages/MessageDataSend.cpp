/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "MessageDataSend.hpp"

#include <ClusterManager.hpp>
#include <MessageDelivery.hpp>

MessageDataSend::MessageDataSend(const ClusterNode *from,
	DataAccessRegion const &remoteRegion,
	ClusterNode *recipient,
	int id)
	: Message(DATA_SEND, sizeof(DataSendMessageContent), from)
{
	_content = reinterpret_cast<DataSendMessageContent *>(_deliverable->payload);
	_content->_remoteRegion = remoteRegion;
	_content->_recipientNodeIndex = recipient->getIndex();
	_content->_id = id;
}

bool MessageDataSend::handleMessage()
{
	ClusterMemoryNode *memoryPlace = ClusterManager::getMemoryNode(_content->_recipientNodeIndex);
	DataTransfer *dt = ClusterManager::sendDataRaw(_content->_remoteRegion, memoryPlace, _content->_id);
	ClusterPollingServices::PendingQueue<DataTransfer>::addPending(dt);

	return true;
}

static const bool __attribute__((unused))_registered_dsend =
	Message::RegisterMSGClass<MessageDataSend>(DATA_SEND);
