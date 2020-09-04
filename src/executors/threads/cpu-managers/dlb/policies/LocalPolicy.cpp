/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include <algorithm>
#include "LocalPolicy.hpp"
#include "executors/threads/cpu-managers/dlb/DLBCPUActivation.hpp"
#include "executors/threads/cpu-managers/dlb/DLBCPUManager.hpp"
#include "ClusterManager.hpp"
#include "ClusterHybridManager.hpp"
#include "monitoring/RuntimeStateMonitor.hpp"

LocalPolicy::LocalPolicy(size_t numCPUs)
	: _numCPUs(numCPUs)
{
	// Time period for local policy in seconds
	ConfigVariable<int> timePeriod("cluster.hybrid.local_time_period");
	double timePeriodSecs = (double)timePeriod.getValue();

	_averagedStats = new AveragedStats(timePeriodSecs, true);   // attached to own stats
}

void LocalPolicy::execute(ComputePlace *cpu, CPUManagerPolicyHint hint, size_t numRequested)
{
	int ownedCores = DLBCPUActivation::getCurrentOwnedCPUs(); // Cores we have owned right now

	// Get average number of busy CPUs
	int ncpus = CPUManager::getTotalCPUs();
	float averageBusyCPUs = _averagedStats->readBusyCores();
	averageBusyCPUs = std::max<float>(2.0, averageBusyCPUs);
	averageBusyCPUs = std::min<float>(averageBusyCPUs, (float)ncpus);
	CPU *currentCPU = (CPU *) cpu;
	ClusterHybridManager::setAveragedBusy(averageBusyCPUs);

	float averagedDemandOtherInstancesSameNode = ClusterHybridManager::getAveragedBusyOtherInstancesSameNode();
	float totalDemandOnNode = averageBusyCPUs + averagedDemandOtherInstancesSameNode;

	int allocCores = 1;
	if (totalDemandOnNode > 0.0) {
		// Scale factor to take account of unused CPUs
		// scale = std::min<double>(scale, 1.1); // do not grow too aggressively
		double scale = ncpus * 1.0 / totalDemandOnNode;
		allocCores = (int)(0.99 + averageBusyCPUs * scale);
		// std::cout << "scaled want is " << numCpusAlloced;
		// Set target allocation to average number of busy CPUs
		ClusterNode *node = ClusterManager::getCurrentClusterNode();
		if (allocCores != node->getCurrentAllocCores()) {
			node->setCurrentAllocCores(allocCores);
		}
	}

	// If too many owned cores, release idle cores via DROM
	if (hint == IDLE_CANDIDATE && ownedCores > allocCores) {
		if (cpu != nullptr) {

			// Prepare this CPU to give to another process. This will
			// immediately drop DLBCPUActivation::getCurrentActiveCPUs.
			if (currentCPU->getActivationStatus() == CPU::enabled_status) {
				DLBCPUActivation::giveCPU(currentCPU);
			}
			return;
		}
	}

	// LeWi policy
	if (hint == IDLE_CANDIDATE) {

		if (cpu != nullptr) {
			// Lend or return the CPU
			CPU::activation_status_t currentStatus = currentCPU->getActivationStatus();
			if (currentStatus == CPU::enabled_status) {
				if (ClusterManager::getTotalReadyTasks() == 0) {
					// Only lend a cpu if there are no ready tasks in this apprank
					DLBCPUActivation::lendCPU(currentCPU);
				}
			} else if (currentStatus == CPU::acquired_enabled_status) {
				if (ClusterManager::getTotalReadyTasks() == 0) {
					// Only return a borrowed cpu (without being reclaimed) if there are no ready tasks in this apprank
					DLBCPUActivation::returnCPU(currentCPU);
				}
			} else if (currentStatus == CPU::shutdown_status) {
				// do nothing
			} else {
				std::cout << "Unexpected CPU state " << DLBCPUActivation::stateNames[currentStatus] << " in LocalPolicy::execute\n";
			}
		}
		return;

	} else if (hint == REQUEST_CPUS) {
		// Try to reclaim CPUs
		int haveCores = DLBCPUActivation::getCurrentActiveOwnedCPUs() + DLBCPUActivation::getCurrentBorrowedCPUs();
		if (haveCores <= 1 || haveCores < ClusterManager::getTotalReadyTasks() + (int)ClusterMetrics::getNumImmovableTasks()) {
			int numReclaimed = DLBCPUActivation::reclaimCPUs(numRequested);
			if (numReclaimed < (int)numRequested) {
				int numLeft = numRequested - numReclaimed;
				// Try to borrow cores
				DLBCPUActivation::acquireCPUs(numLeft);
			}
		}

		return;
	}
}
