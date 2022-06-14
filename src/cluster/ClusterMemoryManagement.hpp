/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_MEMORY_MANAGEMENT_HPP
#define CLUSTER_MEMORY_MANAGEMENT_HPP

#include <nanos6/cluster.h>
#include "messages/MessageDmalloc.hpp"

class MessageDfree;

class ClusterMemoryManagement {

private:
	std::list<MessageDmalloc::MessageDmallocDataInfo *> _dmallocs;

	void registerDmalloc(
		const MessageDmalloc::MessageDmallocDataInfo *dmallocDataInfo,
		Task *task, size_t clusterSize
	);
	bool unregisterDmalloc(DataAccessRegion const &region);

	static ClusterMemoryManagement _singleton;

public:

	static size_t getSerializedDmallocsSize()
	{
		size_t size = 0;
		for (MessageDmalloc::MessageDmallocDataInfo *it : _singleton._dmallocs) {
			size += it->getSize();
		}
		return size;
	}

	static void redistributeDmallocs(size_t newsize);

	static void handleDmallocMessage(const MessageDmalloc *msg, Task *task);
	static void handleDfreeMessage(const MessageDfree *msg);

	static void *dmalloc(
		size_t size,
		nanos6_data_distribution_t policy,
		size_t numDimensions,
		size_t *dimensions
	);

	static void dfree(void *ptr, size_t size);

	static void *lmalloc(size_t size);

	static void lfree(void *ptr, size_t size);
};

#endif /* CLUSTER_MEMORY_MANAGEMENT_HPP */
