/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020-2021 Barcelona Supercomputing Center (BSC)
*/

#include <functional>
#include "ClusterManager.hpp"
#include "ClusterHybridManager.hpp"
#include "executors/threads/CPUManager.hpp"

bool ClusterHybridManager::_inHybridClusterMode = false;
ClusterHybridInterface *ClusterHybridManager::_hyb = nullptr;

void ClusterHybridManager::preinitialize(
	bool forceHybrid,
	int externalRank,
	int apprankNum,
	__attribute__((unused)) int internalRank,
	__attribute__((unused)) int physicalNodeNum,
	__attribute__((unused)) int indexThisPhysicalNode)
{
	_inHybridClusterMode = forceHybrid; // default policy: in hybrid if >1 apprank

	_hyb = GenericFactory<std::string, ClusterHybridInterface*>::getInstance().create("hybrid-file-interface");
	assert(_hyb != nullptr);

	/*
	 * Initialize hybrid interface: controls data distribution within the apprank
	 */
	_hyb->initialize(externalRank, apprankNum);
}

void ClusterHybridManager::initialize()
{
	/*
	 * Start assuming that each cluster node has a sensible initial number of
	 * cores.  This only affects the distribution of work until we get told the
	 * actual number of cores.
	 */
	bool inHybridMode = ClusterHybridManager::inHybridClusterMode();
	const int nodeIndex = ClusterManager::getCurrentClusterNode()->getIndex();
	int myNumCores = CPUManager::getTotalCPUs();

	assert(myNumCores >= 1);
	int clusterSize = ClusterManager::clusterSize();
	for (int i = 0; i < clusterSize; i++) {
		int numCores;
		if (inHybridMode) {
			// In hybrid mode: start by assuming 1 core for masters and 0 cores for slaves.
			// The outcome will be that (1) all dmallocs are distributed with affinity 100% on
			// the current node, and (2) the slaves will only request enough work to keep 1 core busy.
			// This will until the global core allocator runs for the first time.
			numCores = (i == nodeIndex) ? 1 : 0;
		} else {
			// Non-hybrid mode: assume every instance has the same number of cores as this instance, for fair
			// distribution of load
			numCores = myNumCores;
		}
		ClusterManager::getClusterNode(i)->setCurrentAllocCores(numCores);
	}
	if (_hyb) {
		_hyb->writeMapFile();
	}
}
