/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "MessageDataFetch.hpp"

#include <ClusterManager.hpp>
#include <MessageDelivery.hpp>

MessageDataFetch::MessageDataFetch(const ClusterNode *from, DataAccessRegion const &remoteRegion)
	: Message(DATA_FETCH, getMessageContentSizeFromRegion(remoteRegion), from)
{
	_content = reinterpret_cast<DataFetchMessageContent *>(_deliverable->payload);

	const size_t nFragments = getMPIFragments(remoteRegion);
	_content->_nregions = nFragments;

	char *start = (char *)remoteRegion.getStartAddress();

	// if we fragmented the region then more regions are required
	for (size_t i = 0; start < remoteRegion.getEndAddress(); ++i) {

		assert(i < nFragments);
		char *end = (char *) remoteRegion.getEndAddress();

		char *tmp = start + ClusterManager::getMessageMaxSize();
		if (tmp < end) {
			end = tmp;
		}

		_content->_remoteRegionInfo[i]._id = (i == 0 ? getId() : MessageId::nextMessageId());
		_content->_remoteRegionInfo[i]._remoteRegion = DataAccessRegion(start, end);

		start = tmp;
	}
}

bool MessageDataFetch::handleMessage()
{
	ClusterMemoryNode *memoryPlace = ClusterManager::getMemoryNode(getSenderId());
	assert(memoryPlace != nullptr);

	const size_t nFragments = _content->_nregions;

	for (size_t i = 0; i < nFragments; ++i) {

		DataTransfer *dt =
			ClusterManager::sendDataRaw(
				_content->_remoteRegionInfo[i]._remoteRegion,
				memoryPlace,
				_content->_remoteRegionInfo[i]._id
			);

		ClusterPollingServices::PendingQueue<DataTransfer>::addPending(dt);
	}

	return true;
}


size_t MessageDataFetch::getMPIFragments(DataAccessRegion const &remoteRegion)
{
	const size_t totalSize = remoteRegion.getSize();
	assert(totalSize > 0);

	const size_t maxRegionSize = ClusterManager::getMessageMaxSize();

	return (totalSize + maxRegionSize - 1) / maxRegionSize;
}


static Message *createDataFetchMessage(Message::Deliverable *dlv)
{
	return new MessageDataFetch(dlv);
}

static const bool __attribute__((unused))_registered_dfetch =
	REGISTER_MSG_CLASS(DATA_FETCH, createDataFetchMessage);
