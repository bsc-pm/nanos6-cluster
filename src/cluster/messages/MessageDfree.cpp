/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "MessageDfree.hpp"

#include <ClusterManager.hpp>
#include "ClusterMemoryManagement.hpp"
#include <DataAccessRegion.hpp>
#include <memory/directory/cluster/DistributionPolicy.hpp>

MessageDfree::MessageDfree(DataAccessRegion &region) :
	Message(DFREE, sizeof(DfreeMessageContent))
{
	_content = reinterpret_cast<DfreeMessageContent *>(_deliverable->payload);
	_content->_region = region;
}

bool MessageDfree::handleMessage()
{
	assert(_content->_region.getStartAddress() != nullptr);
	assert(_content->_region.getSize() > 0);

	ClusterMemoryManagement::handleDfreeMessage(this);

	return true;
}


static const bool __attribute__((unused))_registered_dfree =
	Message::RegisterMSGClass<MessageDfree>(DFREE);
