/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include <algorithm>
#include "GlobalPolicy.hpp"
#include "executors/threads/cpu-managers/dlb/DLBCPUActivation.hpp"
#include "executors/threads/cpu-managers/dlb/DLBCPUManager.hpp"
#include "ClusterManager.hpp"
#include "ClusterHybridManager.hpp"

// There is no way to return a core when it is actually busy.
//
// Advantages:
//
//  * More robust to bad core allocation:
//        If the number of cores has recently been
//        reduced, but there are still many immovable tasks
//        then there may be a spurious bottleneck where all
//        ranks wait for one rank that has a "small" amount
//        of remaining work but an insufficient number of cores.
//
//  Disadvantages:
//
//  * Does not follow global policy correctly
//        i.e. at the beginning of an iteration, a rank
//        may keep resources and not give it to the rank
//        that will ultimately be on the critical path. But
//        this might only be an issue if the rank that 
//        should return resources has been working until the
//        end of the iteration.
// 

void GlobalPolicy::execute(ComputePlace *cpu, CPUManagerPolicyHint hint, size_t numRequested)
{
	//size_t actualCores = ClusterHybridManager::countEnabledCPUs(); // Cores we currently have according to DROM
	int ownedCores = DLBCPUActivation::getCurrentOwnedCPUs(); // Cores we have owned right now
	int allocCores = ClusterManager::getCurrentClusterNode()->getCurrentAllocCores(); // Cores we are allocated
	CPU *currentCPU = (CPU *) cpu;

	if (hint == IDLE_CANDIDATE && cpu != nullptr) {
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		ClusterStats::hintAsIdle(currentThread);
	}

	// If too many owned cores, release idle cores via DROM
	if (DLBCPUManager::getDromEnabled()) {
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
				DLBCPUActivation::returnCPU(currentCPU);
			} else if (currentStatus == CPU::enabling_status) {
				// temporarily possible
			} else if (currentStatus == CPU::shutdown_status) {
				// do nothing
			} else {
				std::cout << "Unexpected CPU state " << DLBCPUActivation::stateNames[currentStatus] << " in GlobalPolicy::execute\n";
			}
		}
		return;

	} else if (hint == REQUEST_CPUS) {
		// Try to reclaim CPUs
		int numReclaimed = DLBCPUActivation::reclaimCPUs(numRequested);
		if (numReclaimed < (int)numRequested) {
			int numLeft = numRequested - numReclaimed;
			// Try to borrow cores
			DLBCPUActivation::acquireCPUs(numLeft);
		}
		return;

	} else if (hint == HANDLE_TASKFOR) { // hint = HANDLE_TASKFOR
		// Do nothing yet
		assert(cpu != nullptr);
		return; 

	}

	// Do nothing!
	return;
}

