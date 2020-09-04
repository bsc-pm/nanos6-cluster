/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020-2021 Barcelona Supercomputing Center (BSC)
*/

#include "ClusterHybridManager.hpp"
#include "hardware/HardwareInfo.hpp"
#include "hardware/hwinfo/HostInfo.hpp"
#include "ClusterManager.hpp"
#include "executors/threads/CPUManager.hpp"

bool ClusterHybridManager::_inHybridClusterMode = false;
int ClusterHybridManager::_numCPUs;

void ClusterHybridManager::initialize(bool forceHybrid)
{
	_inHybridClusterMode = forceHybrid; // default policy: in hybrid if >1 apprank
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
	const std::vector<bool> &mastersThisNode = ClusterManager::getInstancesThisNode();
	int numAll = mastersThisNode.size();
	FatalErrorHandler::failIf(numAll > _numCPUs,
							"Number of instances ", numAll,
							"greater than number of cores ", _numCPUs,
							"on node", ClusterManager::getNodeNum());

	// Count the number of masters
	int numMasters = 0;
	for (bool isMaster : mastersThisNode) {
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
	int indexThisNode = ClusterManager::getIndexThisNode();
	for(int i=0; i < indexThisNode; i++) {
		curCoreIndex += mastersThisNode[i] ? coresPerMaster : coresPerSlave;
	}

	// Check consistency about this instance
	assert(ClusterManager::isMasterNode() == mastersThisNode[indexThisNode]);

	// Find last (plus one) core to be owned by this instance
	int lastCoreIndex;
	assert(indexThisNode < numAll);
	if (indexThisNode == numAll-1) {
		// This is the last instance on the node; take all remaining cores
		// in case of rounding errors
		lastCoreIndex = _numCPUs;
	} else {
		lastCoreIndex = curCoreIndex + (ClusterManager::isMasterNode()? coresPerMaster : coresPerSlave);
	}

	// Make sure using at least one core (and not a negative number!)
	std::cout << "External rank " << ClusterManager::getExternalRank() << ":"
			  << "take CPUs " << curCoreIndex << " to " << lastCoreIndex << "\n";
	assert(lastCoreIndex > curCoreIndex);

	// Write out the CPU set
	CPU_ZERO(set);
	for(int c=curCoreIndex; c<lastCoreIndex; c++) {
		CPU_SET(c, set);
	}
}

/*
 * Count the actual number of active CPUs (according to CPU Manager)
 */
int ClusterHybridManager::countEnabledCPUs(void)
{
	int numEnabled = 0;
	for(int i=0; i<_numCPUs; i++) {
		CPU *cpu = CPUManager::getCPU(i);
		CPU::activation_status_t status = cpu->getActivationStatus();
		if (status == CPU::enabled_status) {
			numEnabled++;
		}
	}
	return numEnabled;
}
