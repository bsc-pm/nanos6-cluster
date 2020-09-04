/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020-2021 Barcelona Supercomputing Center (BSC)
*/

#include <functional>
#include "ClusterManager.hpp"
#include <atomic>
#include "lowlevel/FatalErrorHandler.hpp"
#include "lowlevel/EnvironmentVariable.hpp"
#include "memory/directory/Directory.hpp"
#include "DistributionPolicy.hpp"
#include "ClusterHybridManager.hpp"
#include "executors/threads/CPUManager.hpp"
#include "monitoring/Monitoring.hpp"
#include "hardware/HardwareInfo.hpp"
#include "hardware/hwinfo/HostInfo.hpp"
#include "ClusterManager.hpp"
#include "executors/threads/CPUManager.hpp"
#include "scheduling/Scheduler.hpp"
#include "monitoring/RuntimeStateMonitor.hpp"
#include "executors/threads/cpu-managers/dlb/DLBCPUActivation.hpp"

bool ClusterHybridManager::_inHybridClusterMode = false;
ClusterHybridInterface *ClusterHybridManager::_hyb = nullptr;
int ClusterHybridManager::_numCPUs;
int ClusterHybridManager::_numOwnedCPUs;
ClusterHybridPolicy ClusterHybridManager::_policy = ClusterHybridPolicy::Bad;
bool ClusterHybridManager::_hybridInterfaceFileInitialized;
float ClusterHybridManager::_busyOtherInstancesSameNode = 0.0;
int ClusterHybridManager::_allocOtherInstancesSameNode = 0;
float ClusterHybridManager::_averagedBusyOtherInstancesSameNode = 0.0;
float ClusterHybridManager::_averagedBusy= 0.0;
AveragedStats  *ClusterHybridManager::_averagedStatsBusyOtherInstancesSameNode;

std::atomic<int> countHandleRequestAttempts(0);

void ClusterHybridManager::preinitialize(
	bool forceHybrid,
	int externalRank,
	int apprankNum,
	int internalRank,
	int physicalNodeNum,
	int indexThisPhysicalNode,
	size_t clusterSize,
	const std::vector<int> &internalRankToExternalRank,
	const std::vector<int> &instanceThisNodeToExternalRank)
{
	_inHybridClusterMode = forceHybrid; // default policy: in hybrid if >1 apprank

	_hyb = GenericFactory<std::string, ClusterHybridInterface*>::getInstance().create("hybrid-file-interface");
	assert(_hyb != nullptr);

	/*
	 * Initialize hybrid interface: controls data distribution within the apprank
	 */
	_hyb->initialize(externalRank, apprankNum, internalRank, physicalNodeNum, indexThisPhysicalNode, clusterSize, internalRankToExternalRank, instanceThisNodeToExternalRank);

	// Time period for local policy in seconds
	ConfigVariable<int> timePeriod("cluster.hybrid.local_time_period");
	double timePeriodSecs = (double)timePeriod.getValue();
	_averagedStatsBusyOtherInstancesSameNode = new AveragedStats(timePeriodSecs, false); // will call from ClusterHybrid

	ConfigVariable<std::string> hybridPolicy("cluster.hybrid.policy");
	if (hybridPolicy.getValue() == "default") {
		_policy = ClusterHybridPolicy::Global;
		_inHybridClusterMode = forceHybrid; // default policy: in hybrid if >1 apprank
	} else if (hybridPolicy.getValue() == "global") {
		_policy = ClusterHybridPolicy::Global;
		_inHybridClusterMode = true; // setting policy forces hybrid mode
	} else if (hybridPolicy.getValue() == "local") {
		_policy = ClusterHybridPolicy::Local;
		_inHybridClusterMode = true; // setting policy forces hybrid mode
	} else {
		FatalErrorHandler::warnIf(true,
			"Unknown hybrid cluster policy:",
			hybridPolicy.getValue(),
			". Using default: ",
			"global");
		_policy = ClusterHybridPolicy::Global;
		_inHybridClusterMode = forceHybrid;
	}
	if (_inHybridClusterMode) {
		Monitoring::enableRuntimeStateMonitor();
	}
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

void ClusterHybridManager::getInitialCPUMask(cpu_set_t *set)
{
	// Get number of cores ("CPUs") on this node
	nanos6_device_t hostDevice = nanos6_device_t::nanos6_host_device;
	HostInfo *hostInfo = ((HostInfo *) HardwareInfo::getDeviceInfo(hostDevice));
	assert(hostInfo != nullptr);
	const std::vector<ComputePlace *> &cpus = hostInfo->getComputePlaces();
	_numCPUs = cpus.size();

	// Get information about the instances on this node
	const std::vector<bool> &isMasterThisNode = ClusterManager::getIsMasterThisNode();
	int numAll = isMasterThisNode.size();
	FatalErrorHandler::failIf(numAll > _numCPUs,
							"Number of instances ", numAll,
							"greater than number of cores ", _numCPUs,
							"on node", ClusterManager::getPhysicalNodeNum());

	// Count the number of masters
	int numMasters = 0;
	for (bool isMaster : isMasterThisNode) {
		if (isMaster) {
			numMasters ++;
		}
	}

	// Calculate the number of cores per master
	int numSlaves = numAll - numMasters;
	int coresPerMaster = 0;
	int coresPerSlave = 0;
	if (numMasters >= 1) {
		coresPerMaster = (_numCPUs - numSlaves) / numMasters;
		coresPerSlave = 1;
	} else {
		// No masters on this node: divide up among slaves instead
		coresPerSlave = _numCPUs / numSlaves;
	}

	// Count the cores owned by instances before this one
	int curCoreIndex = 0;
	int indexThisPhysicalNode = ClusterManager::getIndexThisPhysicalNode();
	for(int i=0; i < indexThisPhysicalNode; i++) {
		curCoreIndex += isMasterThisNode[i] ? coresPerMaster : coresPerSlave;
	}

	// Check consistency about this instance
	assert(ClusterManager::isMasterNode() == isMasterThisNode[indexThisPhysicalNode]);

	// Find last (plus one) core to be owned by this instance
	int lastCoreIndex;
	assert(indexThisPhysicalNode < numAll);
	if (indexThisPhysicalNode == numAll-1) {
		// This is the last instance on the node; take all remaining cores
		// in case of rounding errors
		lastCoreIndex = _numCPUs;
	} else {
		lastCoreIndex = curCoreIndex + (ClusterManager::isMasterNode()? coresPerMaster : coresPerSlave);
	}

	// Make sure using at least one core (and not a negative number!)
	// std::cout << "External rank " << ClusterManager::getExternalRank() << ":"
	// 		  << "take CPUs " << curCoreIndex << " to " << lastCoreIndex << "\n";
	assert(lastCoreIndex > curCoreIndex);
	_numOwnedCPUs = lastCoreIndex - curCoreIndex;

	// Write out the CPU set
	CPU_ZERO(set);
	for(int c=curCoreIndex; c<lastCoreIndex; c++) {
		CPU_SET(c, set);
	}
}

int ClusterHybridManager::getCurrentOwnedCPUs()
{
	return DLBCPUActivation::getCurrentOwnedCPUs();
}

int ClusterHybridManager::getCurrentOwnedOrGivingCPUs()
{
	return DLBCPUActivation::getCurrentOwnedOrGivingCPUs();
}

int ClusterHybridManager::getCurrentLentOwnedCPUs()
{
	return DLBCPUActivation::getCurrentLentOwnedCPUs();
}

int ClusterHybridManager::getCurrentBorrowedCPUs()
{
	return DLBCPUActivation::getCurrentBorrowedCPUs();
}

int ClusterHybridManager::getCurrentActiveOwnedCPUs()
{
	return DLBCPUActivation::getCurrentActiveOwnedCPUs();
}
