/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

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
		addLocalReadyTask(task, computePlace, hint);
		return;
	}

	addReadyLocalOrExecuteRemote(distr(eng), task, computePlace, hint);
}
