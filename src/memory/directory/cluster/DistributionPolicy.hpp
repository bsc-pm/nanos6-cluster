/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef DISTRIBUTION_POLICY_HPP
#define DISTRIBUTION_POLICY_HPP

#include <vector>

#include "tasks/Task.hpp"
#include <nanos6/cluster.h>

#include "cluster/ClusterMemoryManagement.hpp"

class DataAccessRegion;

namespace ClusterDirectory {
	//! \brief Register a DataAccessRegion in the Directory
	//!
	//! \param[in] region is the DataAccessRegion to allocate
	//! \param[in] policy is the policy to distribute memory across nodes
	//! \param[in] nrDimensions is the number of policy dimensions
	//! \param[in] dimensions is the dimensions of the distribution policy
	void registerAllocation(
		const ClusterMemoryManagement::DmallocInfo &dmallocInfo, Task *task, size_t clusterSize
	);

	//! \brief Unregister a DataAccessRegion from the Directory
	//!
	//! \param[in] region is the region we unregister
	void unregisterAllocation(DataAccessRegion const &region);
}




#endif /* DISTRIBUTION_POLICY_HPP */
