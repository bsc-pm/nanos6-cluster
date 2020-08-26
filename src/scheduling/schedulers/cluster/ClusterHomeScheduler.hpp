/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_BALANCE_SCHEDULER_HPP
#define CLUSTER_BALANCE_SCHEDULER_HPP

#include "scheduling/schedulers/cluster/ClusterSchedulerInterface.hpp"
#include "system/RuntimeInfo.hpp"

#include <ClusterManager.hpp>

class ClusterHomeScheduler : public ClusterSchedulerInterface::ClusterSchedulerPolicy {
private:
	inline size_t getNodeIdForLocation(MemoryPlace const *location) const
	{
		if (location->getType() == nanos6_host_device) {
			return _interface->getThisNode()->getIndex();
		}

		return location->getIndex();
	}

public:
	ClusterHomeScheduler(ClusterSchedulerInterface * const interface)
		: ClusterSchedulerPolicy("ClusterHomeScheduler", interface)
	{
	}

	~ClusterHomeScheduler()
	{
	}

	int getScheduledNode(
		Task *task,
		ComputePlace *computePlace,
		ReadyTaskHint hint
	) override;
};

static const bool __attribute__((unused))_registered_home_sched =
	ClusterSchedulerInterface::RegisterClusterSchedulerPolicy<ClusterHomeScheduler>(nanos6_cluster_home);

#endif // CLUSTER_BALANCE_SCHEDULER_HPP
