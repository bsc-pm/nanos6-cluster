/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include <vector>

#include "ClusterLocalityScheduler.hpp"
#include "memory/directory/Directory.hpp"
#include "system/RuntimeInfo.hpp"
#include "tasks/Task.hpp"

#include <ClusterManager.hpp>
#include <DataAccessRegistrationImplementation.hpp>
#include <ExecutionWorkflow.hpp>
#include <VirtualMemoryManagement.hpp>

void ClusterLocalityScheduler::addReadyTask(
	Task *task,
	ComputePlace *computePlace,
	ReadyTaskHint hint
) {
	const size_t clusterSize = ClusterManager::clusterSize();

	std::vector<size_t> bytes(clusterSize, 0);
	bool canBeOffloaded = true;

	DataAccessRegistration::processAllDataAccesses(
		task,
		[&](const DataAccess *access) -> bool {
			const MemoryPlace *location = access->getLocation();
			if (location == nullptr) {
				assert(access->isWeak());
				location = Directory::getDirectoryMemoryPlace();
			}

			DataAccessRegion region = access->getAccessRegion();
			if (!VirtualMemoryManagement::isClusterMemory(region)) {
				canBeOffloaded = false;
				return false;
			}

			if (Directory::isDirectoryMemoryPlace(location)) {
				const Directory::HomeNodesArray *homeNodes = Directory::find(region);

				for (const auto &entry : *homeNodes) {
					location = entry->getHomeNode();

					const size_t nodeId = getNodeIdForLocation(location);

					DataAccessRegion subregion = region.intersect(entry->getAccessRegion());
					bytes[nodeId] += subregion.getSize();
				}

				delete homeNodes;
			} else {
				const size_t nodeId = getNodeIdForLocation(location);

				bytes[nodeId] += region.getSize();
			}

			return true;
		}
	);

	if (!canBeOffloaded) {
		_interface->addReadyLocalOrExecuteRemote(
			nanos6_cluster_no_offload,
			task,
			computePlace,
			hint
		);

		return;
	}

	assert(!bytes.empty());
	std::vector<size_t>::iterator it = bytes.begin();
	const size_t nodeId = std::distance(it, std::max_element(it, it + clusterSize));

	_interface->addReadyLocalOrExecuteRemote(nodeId, task, computePlace, hint);
}
