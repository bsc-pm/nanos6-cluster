/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include <vector>

#include "ClusterHomeScheduler.hpp"
#include "memory/directory/Directory.hpp"
#include "system/RuntimeInfo.hpp"
#include "tasks/Task.hpp"

#include <ClusterManager.hpp>
#include <DataAccessRegistrationImplementation.hpp>
#include <ExecutionWorkflow.hpp>
#include <VirtualMemoryManagement.hpp>

int ClusterHomeScheduler::getScheduledNode(
	Task *task,
	ComputePlace *computePlace  __attribute__((unused)),
	ReadyTaskHint hint  __attribute__((unused))
) {
	const size_t clusterSize = ClusterManager::clusterSize();

	std::vector<size_t> bytes(clusterSize, 0);
	bool canBeOffloaded = true;

	/*
	 * Schedule the task on the node that is home for most of its data
	 */
	DataAccessRegistration::processAllDataAccesses(
		task,
		[&](const DataAccess *access) -> bool {

			DataAccessRegion region = access->getAccessRegion();

			/* Cannot offload a task whose data is not all cluster memory */
			if (!VirtualMemoryManagement::isClusterMemory(region)) {
				canBeOffloaded = false;
				return false;  /* don't continue with other accesses */
			}

			/* Read the location from the directory */
			const Directory::HomeNodesArray *homeNodes = Directory::find(region);

			for (const auto &entry : *homeNodes) {
				/* Each entry has a location and a region */
				const MemoryPlace *location = entry->getHomeNode();
				const DataAccessRegion &homeNodeRegion = entry->getAccessRegion();

				/* Find the intersection with the data access region */
				DataAccessRegion subregion = region.intersect(homeNodeRegion);

				/* Count the bytes at the relevant node ID*/
				const size_t nodeId = getNodeIdForLocation(location);
				bytes[nodeId] += subregion.getSize();
			}

			delete homeNodes;

			return true;
		}
	);

	if (!canBeOffloaded) {
		return nanos6_cluster_no_offload;
	}

	assert(!bytes.empty());
	std::vector<size_t>::iterator it = bytes.begin();
	const size_t nodeId = std::distance(it, std::max_element(it, it + clusterSize));

	return nodeId;
}
