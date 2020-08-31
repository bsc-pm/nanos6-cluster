/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "MessageDataSend.hpp"

#include <ClusterManager.hpp>
#include <MessageDelivery.hpp>

MessageDataSend::MessageDataSend(const ClusterNode *from, DataAccessRegion const &remoteRegion)
	: Message(DATA_SEND, sizeof(DataSendMessageContent), from)
{
	_content = reinterpret_cast<DataSendMessageContent *>(_deliverable->payload);
	_content->_remoteRegion = remoteRegion;
}

bool MessageDataSend::handleMessage()
{
	ClusterMemoryNode *memoryPlace = ClusterManager::getMemoryNode(getSenderId());

	DataTransfer *dt = ClusterManager::fetchDataRaw(_content->_remoteRegion, memoryPlace, getId());

	ClusterPollingServices::PendingQueue<DataTransfer>::addPending(dt);

	return true;
}

static Message *createDataSendMessage(Message::Deliverable *dlv)
{
	return new MessageDataSend(dlv);
}

static const bool __attribute__((unused))_registered_dsend =
	REGISTER_MSG_CLASS(DATA_SEND, createDataSendMessage);
