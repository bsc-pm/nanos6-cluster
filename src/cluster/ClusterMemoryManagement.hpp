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

	struct DmallocInfo {

		const DataAccessRegion _region;
		const nanos6_data_distribution_t _policy;
		const size_t _nrDimensions;
		size_t *_dimensions;

		DmallocInfo(
			const DataAccessRegion &region,
			nanos6_data_distribution_t policy, size_t nrDimensions, const size_t *dimensions
		)
			: _region(region), _policy(policy), _nrDimensions(nrDimensions), _dimensions(nullptr)
		{
			if (dimensions != nullptr) {
				assert(_nrDimensions > 0);
				_dimensions = new size_t[_nrDimensions];
				memcpy(_dimensions, _dimensions, _nrDimensions * sizeof(size_t));
			}
		}

		~DmallocInfo()
		{
			if (_dimensions) {
				assert(_nrDimensions > 0);
				delete[] _dimensions;
			}
		}
	};

	std::list<DmallocInfo> _dmallocs;

	void registerDmalloc(const MessageDmalloc::DmallocMessageContent *dmallocInfo, Task *task);
	bool unregisterDmalloc(DataAccessRegion const &region);

	static ClusterMemoryManagement _singleton;

public:

	void redistributeDmallocs(size_t newsize);

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
