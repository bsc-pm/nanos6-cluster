/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_LOCALITY_SCHEDULER_HPP
#define CLUSTER_LOCALITY_SCHEDULER_HPP

#include "scheduling/schedulers/cluster/ClusterSchedulerInterface.hpp"
#include "system/RuntimeInfo.hpp"

#include <ClusterManager.hpp>

class ClusterLocalityScheduler : public ClusterSchedulerInterface {
private:
	inline size_t getNodeIdForLocation(MemoryPlace const *location) const
	{
		if (location->getType() == nanos6_host_device) {
			return _thisNode->getIndex();
		}

		return location->getIndex();
	}

public:
	ClusterLocalityScheduler() : ClusterSchedulerInterface("ClusterLocalityScheduler")
	{
	}

	~ClusterLocalityScheduler()
	{
	}

	void addReadyTask(Task *task, ComputePlace *computePlace, ReadyTaskHint hint = NO_HINT);
};

#endif // CLUSTER_LOCALITY_SCHEDULER_HPP
