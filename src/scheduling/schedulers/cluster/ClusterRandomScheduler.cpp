/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include <random>

#include "ClusterRandomScheduler.hpp"

#include <ClusterManager.hpp>
#include <DataAccessRegistrationImplementation.hpp>
#include <ExecutionWorkflow.hpp>
#include <VirtualMemoryManagement.hpp>

void ClusterRandomScheduler::addReadyTask(
	Task *task,
	ComputePlace *computePlace,
	ReadyTaskHint hint
) {
	if (ClusterSchedulerInterface::handleClusterSchedulerConstrains(task, computePlace, hint)) {
		return;
	}

	bool canBeOffloaded = true;
	DataAccessRegistration::processAllDataAccesses(task,
		[&](const DataAccess *access) -> bool {
			DataAccessRegion region = access->getAccessRegion();
			if (!VirtualMemoryManagement::isClusterMemory(region)) {
				canBeOffloaded = false;
				return false;
			}
			return true;
		}
	);

	if (!canBeOffloaded) {
		SchedulerInterface::addReadyTask(task, computePlace, hint);
		return;
	}

	std::random_device rd;
	std::mt19937 eng(rd());
	std::uniform_int_distribution<> distr(0, _clusterSize - 1);

	addReadyLocalOrExecuteRemote(distr(eng), task, computePlace, hint);
}
