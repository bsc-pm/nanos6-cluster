/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "MessageDataSend.hpp"

#include <ClusterManager.hpp>
#include <MessageDelivery.hpp>

MessageDataSend::MessageDataSend(const ClusterNode *from,
	size_t numSends,
	std::vector<TaskOffloading::DataSendRegionInfo> const &dataSends)
	: Message(DATA_SEND, sizeof(size_t) + numSends * sizeof(DataSendRegionInfoElem), from)
{
	_content = reinterpret_cast<DataSendMessageContent *>(_deliverable->payload);
	_content->_numSends = numSends;
	size_t index = 0;
	for (TaskOffloading::DataSendRegionInfo const &regionInfo : dataSends) {
		_content->_dataSendRegionInfo[index]._remoteRegion = regionInfo._remoteRegion;
		assert(regionInfo._recipient->getType() == nanos6_device_t::nanos6_cluster_device);
		const ClusterMemoryNode *clusterMemoryNode = dynamic_cast<const ClusterMemoryNode *>(regionInfo._recipient);
		_content->_dataSendRegionInfo[index]._recipientNodeIndex = clusterMemoryNode->getIndex();
		_content->_dataSendRegionInfo[index]._id = regionInfo._id;
		index++;
	}
}

bool MessageDataSend::handleMessage()
{
	const size_t numSends = _content->_numSends;
	for(size_t i = 0; i < numSends; i++) {
		DataSendRegionInfoElem const &regionInfo = _content->_dataSendRegionInfo[i];
		ClusterMemoryNode *memoryPlace = ClusterManager::getMemoryNode(regionInfo._recipientNodeIndex);
		DataTransfer *dt = ClusterManager::sendDataRaw(regionInfo._remoteRegion, memoryPlace, regionInfo._id);
		ClusterPollingServices::PendingQueue<DataTransfer>::addPending(dt);
	}

	return true;
}

static const bool __attribute__((unused))_registered_dsend =
	Message::RegisterMSGClass<MessageDataSend>(DATA_SEND);
