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
	void *dptr, size_t size, nanos6_data_distribution_t policy,
	size_t numDimensions, size_t *dimensions
)
	: Message(DMALLOC, sizeof(DmallocMessageContent) + numDimensions * sizeof(size_t), from)
{
	_content = reinterpret_cast<DmallocMessageContent *>(_deliverable->payload);
	_content->_dptr = dptr;
	_content->_allocationSize = size;
	_content->_policy = policy;
	memcpy(_content->_dimensions, dimensions, sizeof(size_t) * _content->_nrDim);
}

bool MessageDmalloc::handleMessage()
{
	if (ClusterManager::isMasterNode()) {
		assert(_content->_dptr == nullptr);

		const size_t size = getAllocationSize();

		_content->_dptr = VirtualMemoryManagement::allocDistrib(size);
		FatalErrorHandler::failIf(_content->_dptr == nullptr,
			"Master node couldn't allocate distributed memory with size: ", size);

		const ClusterNode *node = ClusterManager::getClusterNode(this->getSenderId());
		assert(node != nullptr);

		// We don't change the message so it will keep the original sender and won't be sent to it
		// by the ping-pong protection.
		ClusterManager::sendMessageToAll(this, true);

		// And send only address to the original sender.
		DataAccessRegion region(&_content->_dptr, sizeof(void *));
		ClusterManager::sendDataRaw(region, node->getMemoryNode(), this->getId(), true);
	}

	ClusterMemoryManagement::handle_dmalloc_message(this, nullptr);

	return true;
}

static const bool __attribute__((unused))_registered_dmalloc =
	Message::RegisterMSGClass<MessageDmalloc>(DMALLOC);
