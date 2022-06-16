/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_MEMORY_MANAGEMENT_HPP
#define CLUSTER_MEMORY_MANAGEMENT_HPP

#include <nanos6/cluster.h>

class MessageDmalloc;
class MessageDfree;

class ClusterMemoryManagement {
public:
	struct DmallocDataInfo {
		//! Address pointer.
		DataAccessRegion _region;

		//! Cluster size in allocation moment.
		size_t _clusterSize;

		//! distribution policy for the region
		nanos6_data_distribution_t _policy;

		//! number of dimensions for distribution
		size_t _nrDim;

		//! dimensions of the distribution
		size_t _dimensions[];

		size_t getSize() const
		{
			return sizeof(void*)
				+ 3 * sizeof(size_t)
				+ sizeof(nanos6_data_distribution_t)
				+ _nrDim * sizeof(size_t);
		}

		DmallocDataInfo(
			const DataAccessRegion &region, size_t clusterSize,
			nanos6_data_distribution_t policy, size_t nrDim, const size_t *dimensions
		) : _region(region), _clusterSize(clusterSize), _policy(policy), _nrDim(nrDim)
		{
			memcpy(_dimensions, dimensions, nrDim * sizeof(size_t));
		}
	};

private:
	std::list<DmallocDataInfo *> _dmallocs;

	void registerDmalloc(
		const DmallocDataInfo *dmallocDataInfo,
		Task *task, size_t clusterSize
	);
	bool unregisterDmalloc(DataAccessRegion const &region);

	static ClusterMemoryManagement _singleton;

public:

	static inline const std::list<DmallocDataInfo *> &getMallocsList()
	{
		return _singleton._dmallocs;
	}

	static size_t getSerializedDmallocsSize()
	{
		size_t size = 0;
		for (DmallocDataInfo *it : _singleton._dmallocs) {
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
