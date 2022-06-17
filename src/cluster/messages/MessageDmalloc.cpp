/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "MessageDmalloc.hpp"

#include <list>

#include <ClusterManager.hpp>
#include "ClusterMemoryManagement.hpp"
#include <DistributionPolicy.hpp>
#include <VirtualMemoryManagement.hpp>

MessageDmalloc::MessageDmalloc(
	const DataAccessRegion &region, size_t clusterSize,
	nanos6_data_distribution_t policy, size_t nrDim, const size_t *dimensions
)
	: Message(DMALLOC,
		2 * sizeof(size_t)
		+ sizeof(ClusterMemoryManagement::DmallocDataInfo) + nrDim * sizeof(size_t))
{
	_content = reinterpret_cast<DmallocMessageContent *>(_deliverable->payload);
	_content->_ndmallocs = 1;
	_content->getOffsetPtr()[0] = 0;
	ClusterMemoryManagement::DmallocDataInfo *ptr = _content->getData(0);

	new (ptr) ClusterMemoryManagement::DmallocDataInfo(
		region, clusterSize, policy, nrDim, dimensions
	);
}

MessageDmalloc::MessageDmalloc(const ClusterMemoryManagement::dmalloc_container_t &dmallocs)
	: Message(DMALLOC,
		sizeof(size_t)
		+ dmallocs.size() * sizeof(size_t)
		+ ClusterMemoryManagement::getSerializedDmallocsSize())
{
	_content = reinterpret_cast<DmallocMessageContent *>(_deliverable->payload);
	_content->_ndmallocs = dmallocs.size();

	size_t i = 0;
	size_t offset = 0;
	for (const ClusterMemoryManagement::DmallocDataInfo *it : dmallocs) {
		_content->getOffsetPtr()[i] = offset;
		ClusterMemoryManagement::DmallocDataInfo *data = _content->getData(i);

		new (data) ClusterMemoryManagement::DmallocDataInfo(
			it->_region,
			it->_clusterSize,
			it->_policy,
			it->_nrDim,
			it->_dimensions
		);

		assert(it->getSize() == data->getSize());

		offset += it->getSize();
		++i;
	}
}

bool MessageDmalloc::handleMessage()
{
	if (ClusterManager::isMasterNode()) {
		assert(_content->_ndmallocs == 1);

		ClusterMemoryManagement::DmallocDataInfo *data = this->getContent()->getData(0);

		assert(data->_region.getStartAddress() == nullptr);

		const size_t allocationSize = data->_region.getSize();

		void *dptr = VirtualMemoryManagement::allocDistrib(allocationSize);
		FatalErrorHandler::failIf(dptr == nullptr,
			"Master node couldn't allocate distributed memory with size: ", allocationSize);

		data->_region = DataAccessRegion(dptr, allocationSize);

		const ClusterNode *node = ClusterManager::getClusterNode(this->getSenderId());
		assert(node != nullptr);

		// We don't change the message so it will keep the original sender and won't be sent to it
		// by the ping-pong protection.
		ClusterManager::sendMessageToAll(this, true);

		// And send only address to the original sender.
		DataAccessRegion region(&dptr, sizeof(void *));
		ClusterManager::sendDataRaw(region, node->getMemoryNode(), this->getId(), true);
	}

	ClusterMemoryManagement::handleDmallocMessage(this, nullptr);

	return true;
}

static const bool __attribute__((unused))_registered_dmalloc =
	Message::RegisterMSGClass<MessageDmalloc>(DMALLOC);
