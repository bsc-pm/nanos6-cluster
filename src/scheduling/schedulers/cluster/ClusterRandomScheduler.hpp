/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_RANDOM_SCHEDULER_HPP
#define CLUSTER_RANDOM_SCHEDULER_HPP

#include <random>

#include "scheduling/schedulers/cluster/ClusterSchedulerInterface.hpp"
#include "system/RuntimeInfo.hpp"

#include <ClusterManager.hpp>

class ClusterRandomScheduler : public ClusterSchedulerInterface::ClusterSchedulerPolicy {

private:
	std::random_device rd;
	std::mt19937 eng;
	std::uniform_int_distribution<> distr;

public:
	ClusterRandomScheduler(ClusterSchedulerInterface * const interface)
		: ClusterSchedulerInterface::ClusterSchedulerPolicy("ClusterRandomScheduler", interface),
		eng(rd()),
		distr(0, ClusterManager::clusterSize() - 1)
	{
	}

	~ClusterRandomScheduler()
	{
	}

	int getScheduledNode(
		Task *task,
		ComputePlace *computePlace,
		ReadyTaskHint hint = NO_HINT
	) override;
};

#endif // CLUSTER_RANDOM_SCHEDULER_HPP
