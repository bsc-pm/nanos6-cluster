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
public:
	struct DmallocInfo {

		const DataAccessRegion _region;
		const nanos6_data_distribution_t _policy;
		// cluster size on allocation moment, spawning and shrinking need to remember this.
		const size_t _initialClusterSize;
		const std::vector<size_t> _dimensions;

		DmallocInfo(const MessageDmalloc::MessageDmallocDataInfo *dmallocDataInfo)
			: _region(dmallocDataInfo->_dptr, dmallocDataInfo->_allocationSize),
			  _policy(dmallocDataInfo->_policy),
			  _initialClusterSize(dmallocDataInfo->_clusterSize),
			  _dimensions(dmallocDataInfo->_dimensions,
				  dmallocDataInfo->_dimensions + dmallocDataInfo->_nrDim)
		{
		}
	};

private:
	std::list<DmallocInfo> _dmallocs;

	void registerDmalloc(const DmallocInfo &dmallocInfo, Task *task, size_t clusterSize);
	bool unregisterDmalloc(DataAccessRegion const &region);

	static ClusterMemoryManagement _singleton;

public:

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
