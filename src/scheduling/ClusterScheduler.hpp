/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_SCHEDULER_HPP
#define CLUSTER_SCHEDULER_HPP

#include <string>

#include "SchedulerInterface.hpp"
#include "LocalScheduler.hpp"
#include "schedulers/cluster/ClusterLocalityScheduler.hpp"
#include "schedulers/cluster/ClusterHomeScheduler.hpp"
#include "schedulers/cluster/ClusterBalanceScheduler.hpp"
#include "schedulers/cluster/ClusterRandomScheduler.hpp"
#include "system/RuntimeInfo.hpp"

#include <ClusterManager.hpp>
#include <ClusterHybridManager.hpp>

class ClusterScheduler {

public:
	static SchedulerInterface *generate(const std::string &name)
	{
		if (ClusterManager::inClusterMode()) {

			if (name == "random") {
				return new ClusterSchedulerInterface(nanos6_cluster_random);
			}

			if (name == "locality") {
				return new ClusterSchedulerInterface(nanos6_cluster_locality);
			}

			if (name == "home") {
				return new ClusterSchedulerInterface(nanos6_cluster_home);
			}

			if (name == "balance") {
				return new ClusterSchedulerInterface(nanos6_cluster_balance);
			}

			// This is the default.
			nanos6_cluster_scheduler_t defaultScheduler;
			if (ClusterHybridManager::inHybridClusterMode())  {
				defaultScheduler = nanos6_cluster_balance;
			} else {
				defaultScheduler = nanos6_cluster_locality;
			}
			SchedulerInterface *ret = new ClusterSchedulerInterface(defaultScheduler);

			// This is the default.
			FatalErrorHandler::warn(
				"Unknown cluster scheduler:", name, ". Using default: ", ret->getName()
			);

			return ret;
		} else {
			return new LocalScheduler();
		}
	}

};

#endif // CLUSTER_SCHEDULER_HPP
