/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "MessageDmalloc.hpp"

#include <ClusterManager.hpp>
#include "ClusterMemoryManagement.hpp"
#include <DistributionPolicy.hpp>
#include <VirtualMemoryManagement.hpp>

MessageDmalloc::MessageDmalloc(const ClusterNode *from,
	void *dptr, size_t size, size_t clusterSize, nanos6_data_distribution_t policy,
	size_t nrDim, size_t *dimensions
)
	: Message(DMALLOC,
		2 * sizeof(size_t)
		+ sizeof(MessageDmallocDataInfo) + nrDim * sizeof(size_t),
		from)
{
	_content = reinterpret_cast<DmallocMessageContent *>(_deliverable->payload);
	_content->_ndmallocs = 1;
	_content->getOffsetPtr()[0] = 0;
	MessageDmallocDataInfo *ptr = _content->getData(0);

	new (ptr) MessageDmallocDataInfo(dptr, size, clusterSize, policy, nrDim, dimensions);
}

bool MessageDmalloc::handleMessage()
{
	if (ClusterManager::isMasterNode()) {
		assert(_content->_ndmallocs == 1);

		MessageDmallocDataInfo *data = this->getContent()->getData(0);

		assert(data->_dptr == nullptr);

		const size_t allocationSize = data->_allocationSize;

		data->_dptr = VirtualMemoryManagement::allocDistrib(allocationSize);
		FatalErrorHandler::failIf(data->_dptr == nullptr,
			"Master node couldn't allocate distributed memory with size: ", allocationSize);

		const ClusterNode *node = ClusterManager::getClusterNode(this->getSenderId());
		assert(node != nullptr);

		// We don't change the message so it will keep the original sender and won't be sent to it
		// by the ping-pong protection.
		ClusterManager::sendMessageToAll(this, true);

		// And send only address to the original sender.
		DataAccessRegion region(&data->_dptr, sizeof(void *));
		ClusterManager::sendDataRaw(region, node->getMemoryNode(), this->getId(), true);
	}

	ClusterMemoryManagement::handleDmallocMessage(this, nullptr);

	return true;
}

static const bool __attribute__((unused))_registered_dmalloc =
	Message::RegisterMSGClass<MessageDmalloc>(DMALLOC);
