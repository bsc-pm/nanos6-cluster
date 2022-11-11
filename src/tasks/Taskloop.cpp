/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "Taskloop.hpp"
#include "tasks/LoopGenerator.hpp"
#include "ClusterManager.hpp"

void Taskloop::createTaskloopOffloaders(Task *parent)
{
	// Distribute this taskloop across all of the cluster nodes. We do this
	// by creating one "taskloop offloader" per node, which has a part of the original
	// loop bounds. The taskloop offloader will get offloaded to the relevant node
	// (because createTaskloopOffloader will call Task::setNode()). We could have
	// a cleverer distribution policy, taking account of locality or affinity, but this
	// simple policy is a good start. If you don't want to distribute it, then provide
	// a node clause with any argument other than nanos6_cluster_no_hint, which will
	// control the scheduling of the taskloop as a whole.
	int numNodes = ClusterManager::clusterSize();
	unsigned int lb = _bounds.lower_bound;
	unsigned int ub = _bounds.upper_bound;
	assert(ub > lb);
	unsigned int itersPerNode = (ub - lb + numNodes-1) / numNodes;

	// Generate one taskloop offloader per node (which will get executed on that node)
	for(int j = 0; j < numNodes; j++) {
		Taskloop::bounds_t bounds;
		bounds.lower_bound = lb;
		bounds.upper_bound = std::min<unsigned int>(lb+itersPerNode, ub);
		bounds.chunksize = _bounds.chunksize;
		bounds.grainsize = _bounds.grainsize;

		// Create a taskloop offloader to be offloaded to node j (or executed locally)
		LoopGenerator::createTaskloopOffloader(this, parent, bounds, ClusterManager::getClusterNode(j));

		lb = bounds.upper_bound;
	}
}

void Taskloop::body(nanos6_address_translation_entry_t *translationTable)
{
	if (!isTaskloopSource()) {
		// Taskloop executor: execute the loop body
		getTaskInfo()->implementations[0].run(getArgsBlock(), &getBounds(), translationTable);
	} else {

		if (isRemoteTask()		// Taskloop was offloaded
			|| _offloader		// It is a taskloop offloader that for some reason didn't get offloaded
			|| (this->getConstraints()->node != nanos6_cluster_no_hint)) {	// there is a node clause: all on same node

			// Update grainsize for #iterations executed on this node
			calculateGrainsize(_bounds);
			// Generate the taskloop executors for the given loop bounds
			while (getIterationCount() > 0) {
				LoopGenerator::createTaskloopExecutor(this, _bounds);
			}
		} else {
			createTaskloopOffloaders(this);
		}
	}
}
