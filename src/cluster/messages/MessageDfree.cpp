/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "MessageDfree.hpp"

#include <ClusterManager.hpp>
#include "ClusterMemoryManagement.hpp"
#include <DataAccessRegion.hpp>
#include <memory/directory/cluster/DistributionPolicy.hpp>

MessageDfree::MessageDfree(const ClusterNode *from, void *address, size_t size) :
	Message(DFREE, sizeof(DfreeMessageContent), from)
{
	_content = reinterpret_cast<DfreeMessageContent *>(_deliverable->payload);
	_content->_address = address;
	_content->_size = size;
}

bool MessageDfree::handleMessage()
{
	DataAccessRegion region(_content->_address, _content->_size);

	//! TODO: We need to fix the way we allocate distributed memory so that
	//! we do allocate it from the MemoryAllocator instead of the
	//! VirtualMemoryManagement layer, which is what we do now. The
	//! VirtualMemoryManagement layer does not allow (at the moment)
	//! deallocation of memory, so for now we do not free distributed
	//! memory

	//! Unregister the region from the home node map
	ClusterDirectory::unregisterAllocation(region);
	ClusterMemoryManagement::unregisterDmalloc(region);

	ClusterManager::synchronizeAll();

	return true;
}


static const bool __attribute__((unused))_registered_dfree =
	Message::RegisterMSGClass<MessageDfree>(DFREE);
