/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include <algorithm>
#include <dlb_drom.h>
#include <cassert>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string.h>
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "ClusterManager.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "ClusterHybridInterfaceFile.hpp"
#include "ClusterHybridManager.hpp"
#include "ClusterMemoryManagement.hpp"
#include "scheduling/Scheduler.hpp"
#include <executors/threads/CPUManager.hpp>
#include "DLBCPUActivation.hpp"
#include "InstrumentCluster.hpp"
#include "cluster/ClusterMetrics.hpp"
#include "monitoring/Monitoring.hpp"
#include "monitoring/RuntimeStateMonitor.hpp"

#pragma GCC visibility push(default)
#include <mpi.h>
#pragma GCC visibility pop

ClusterHybridInterfaceFile::ClusterHybridInterfaceFile() :
	_allocFileThisApprank(nullptr)
{
	ConfigVariable<std::string> clusterHybridDirectory("cluster.hybrid.directory");
	std::string s = clusterHybridDirectory.getValue();
	_directory = strdup(s.c_str());
	// Get current time for polling service
	readTime(&_prevTime);
}

void ClusterHybridInterfaceFile::initialize(int externalRank,
											int apprankNum,
											int internalRank,
											__attribute__((unused)) int physicalNodeNum,
											__attribute__((unused)) int indexThisPhysicalNode,
											int clusterSize,
											const std::vector<int> &internalRankToExternalRank,
											const std::vector<int> &instanceThisNodeToExternalRank)
{
	// External rank 0 clears or creates the .hybrid/ directory (NOTE: cannot
	// call ClusterManager::getExternalRank() as this function is called during
	// the initialization of ClusterManager)
	if (externalRank  == 0) {
		struct stat sb;
		if (stat(_directory, &sb) == 0 && S_ISDIR(sb.st_mode)) {
			// .hybrid/ directory already exists: clear all map, utilization and alloc files inside it
			// (keep other things that may exist in the directory, like rebalance output)
			DIR *dirStream = opendir(_directory);
			struct dirent *file;
			while ( (file = readdir(dirStream)) != nullptr) {
				if (strncmp(file->d_name, "map", strlen("map")) == 0
					|| strncmp(file->d_name, "alloc", strlen("alloc")) == 0
					|| strncmp(file->d_name, "utilization", strlen("utilization")) == 0) {
					std::stringstream ss;
					ss << _directory << "/" << file->d_name;
					remove(ss.str().c_str());
				}
			}
			closedir(dirStream);
		} else {
			// Create empty .hybrid directory
			int ret = mkdir(_directory, 0777);
			FatalErrorHandler::failIf(
				ret != 0,
				"Cannot create ", _directory, " directory for hybrid MPI + OmpSs-2@Cluster"
				);
		}
	}

	// After this barrier, all ranks can assume that the .hybrid/ directory
	// has been created by the first external rank
	MPI_Barrier(MPI_COMM_WORLD);

	// Filenames for this apprank's core allocation (using global policy)
	// We cannot call ClusterManager::getApprankNum() as this function is
	// called during the initialization of ClusterManager.
	std::stringstream ss1;
	ss1 << _directory << "/alloc" << apprankNum;
	_allocFileThisApprank = strdup(ss1.str().c_str());

	/*
	 * Open file to write this instance's utilization
	 */
	std::stringstream ss2;
	ss2 << _directory << "/utilization" << externalRank;
	std::string s2 = ss2.str();
	const char *utilizationFilename = s2.c_str();
	_utilizationFile.open(utilizationFilename);

	MPI_Barrier(MPI_COMM_WORLD);
	sleep(1);

	/*
	 * For local policy: vector of ifstreams to read the utilization of the
	 * other internal ranks in this apprank
	 */
	_utilizationOtherRanksInApprank.resize(clusterSize);
	for (int i=0; i<clusterSize; i++) {
		if (i == internalRank) {
			_utilizationOtherRanksInApprank[i] = nullptr;
		} else {
			_utilizationOtherRanksInApprank[i] = new std::ifstream;
			std::stringstream ss3;
			int otherExternalRank = internalRankToExternalRank[i];
			ss3 << _directory << "/utilization" << otherExternalRank;
			std::string s3 = ss3.str();
			const char *otherUtilizationFilename = s3.c_str();
			_utilizationOtherRanksInApprank[i]->open(otherUtilizationFilename);
		}
	}
	/*
	 *
	 */
	int numThisNode = instanceThisNodeToExternalRank.size();
	_utilizationOtherRanksThisNode.resize(numThisNode);
	for (int i=0; i<numThisNode; i++) {
		int otherExternalRank = instanceThisNodeToExternalRank[i];
		if (otherExternalRank == externalRank) {
			_utilizationOtherRanksThisNode[i] = nullptr;
		} else {
			_utilizationOtherRanksThisNode[i] = new std::ifstream;
			std::stringstream ss3;
			ss3 << _directory << "/utilization" << otherExternalRank;
			std::string s3 = ss3.str();
			const char *otherUtilizationFilename = s3.c_str();
			_utilizationOtherRanksThisNode[i]->open(otherUtilizationFilename);
		}
	}
}

void ClusterHybridInterfaceFile::writeMapFile(void)
{
	// Now create our map file
	std::stringstream ss0;
	assert(CPUManager::isPreinitialized()); // need CPUManager::getTotalCPUs()
	int externalRank = ClusterManager::getExternalRank();
	ss0 << _directory << "/map" << externalRank;
	std::ofstream mapFile(ss0.str().c_str());
	mapFile << "externalRank " << externalRank << "\n"
		<< "apprankNum " << ClusterManager::getApprankNum() << "\n"
		<< "internalRank " << ClusterManager::getCurrentClusterNode()->getIndex() << "\n"
		<< "nodeNum " << ClusterManager::getPhysicalNodeNum() << "\n"
		<< "indexThisPhysicalNode " << ClusterManager::getIndexThisPhysicalNode() << "\n"
		<< "cpusOnNode " << CPUManager::getTotalCPUs() << "\n";
	mapFile.close();
}

bool ClusterHybridInterfaceFile::updateAllocFileGlobal(void)
{
	bool changed = false;
	std::ifstream allocFile(_allocFileThisApprank);
	if (allocFile.is_open())
	{
		std::vector <ClusterNode *> const &clusterNodes = ClusterManager::getClusterNodes();
		for(ClusterNode *node : clusterNodes) {
			int ncores = -1;
			allocFile >> ncores;
			if (ncores >= 0) {
				if (ncores != node->getCurrentAllocCores()) {
					node->setCurrentAllocCores(ncores);
					changed = true;
					if (node == ClusterManager::getCurrentClusterNode()) {
						Instrument::emitClusterEvent(Instrument::ClusterEventType::AllocCores, ncores);
					}
				}
			}
		}
        allocFile.close();
	}

	return changed;
}

int ClusterHybridInterfaceFile::updateTotalsThisNode(void)
{
	int numInstancesThisNode = _utilizationOtherRanksThisNode.size();
	int totalAlloc = 0;
	float totalBusy = 0.0;
	bool ok = false;

	for (int index=0; index < numInstancesThisNode; index++) {
		int externalRank = ClusterManager::getExternalRankThisNode(index);
		if (externalRank != ClusterManager::getExternalRank()) {
			std::ifstream *utilFile = _utilizationOtherRanksThisNode[index];

			assert(utilFile);
			assert(utilFile->is_open());

			std::string line;
			int allocCores = 0;
			float busyCores = 0;
			while (std::getline(*utilFile, line)) {
				std::istringstream iss(line);
				float timestamp;
				int enabledCores;
				float usefulBusyCores;
				int numReadyTasks;
				int ignore; // its view of total tasks
				int totalBusyCoresThatApprank;
				int numImmovableTasks;
				iss >> timestamp >> allocCores >> enabledCores >> busyCores >> usefulBusyCores >> numReadyTasks
						>> ignore >> totalBusyCoresThatApprank >> numImmovableTasks;


				ok = true; // really only good if one other rank
			}
			utilFile->clear(); // clear EOF condition so can try reading again next time
			totalAlloc += allocCores;
			totalBusy += busyCores;
		}
	}
	if (ok) {
		ClusterHybridManager::setDemandOtherInstancesSameNode(totalAlloc, totalBusy);
	}
	return totalAlloc;
}

bool ClusterHybridInterfaceFile::updateNumbersOfCores(bool isLocal, float totalBusyCores)
{
	bool changed = false;
	int totalReadyTasks = ClusterMetrics::getNumReadyTasks() - ClusterMetrics::getNumImmovableTasks();
	std::vector <ClusterNode *> const &clusterNodes = ClusterManager::getClusterNodes();

	for(ClusterNode *node : clusterNodes) {
		if (node != ClusterManager::getCurrentClusterNode()) {
			int internalRank = node->getIndex();
			std::ifstream *utilFile = _utilizationOtherRanksInApprank[internalRank];

			assert (utilFile->is_open());

				std::string line;
				while (std::getline(*utilFile, line)) {
					std::istringstream iss(line);
					float timestamp;
					int allocCores;
					int enabledCores;
					float busyCores;
					float usefulBusyCores;
					int numReadyTasks;
					int ignore; // its view of total tasks
					int ignore2;
					int numImmovableTasks;
					iss >> timestamp >> allocCores >> enabledCores >> busyCores >> usefulBusyCores >> numReadyTasks
							>> ignore >> ignore2 >> numImmovableTasks;

					if (isLocal) {
						// With local policy: read current allocation from the utilization file
						if (allocCores >= 0) {
							if (allocCores != node->getCurrentAllocCores()) {
								node->setCurrentAllocCores(allocCores);
								changed = true;
							}
						}
					}
					node->setCurrentEnabledCores(enabledCores);
					node->setCurrentBusyCores(busyCores);

					int offloadableReadyTasks = numReadyTasks - numImmovableTasks;
					node->setCurrentReadyTasks(offloadableReadyTasks);
				}
				utilFile->clear(); // clear EOF condition so can try reading again next time

			totalReadyTasks += node->getCurrentReadyTasks();
			totalBusyCores += node->getCurrentBusyCores();
		}
	}
	ClusterManager::setTotalReadyTasks(totalReadyTasks);
	ClusterMetrics::setTotalBusyCoresCurrentApprank(totalBusyCores);

	if (ClusterHybridManager::inHybridClusterMode()) {
		if (totalReadyTasks > 0) { // Try to get a CPU for our apprank
			// Task = nullptr only supported for the hybrid cluster mode DLB policies
			CPUManager::executeCPUManagerPolicy(nullptr, REQUEST_CPUS, 1);
		} else {
			// Task = nullptr only supported for the hybrid cluster mode DLB policies
			CPUManager::executeCPUManagerPolicy(nullptr, IDLE_CANDIDATE, 1);
		}
	}

	return changed;
}

void ClusterHybridInterfaceFile::appendUtilization(float timestamp, float totalBusyCores, float usefulBusyCores)
{
	int allocCores = ClusterManager::getCurrentClusterNode()->getCurrentAllocCores();
	int numCpusOwned = DLBCPUActivation::getCurrentOwnedOrGivingCPUs();
	int numLentOwned = DLBCPUActivation::getCurrentLentOwnedCPUs();
	int numBorrowed  = DLBCPUActivation::getCurrentBorrowedCPUs();
	int enabledCores = numCpusOwned - numLentOwned + numBorrowed;

	int otherAlloc = 0;
	int numOffloaded = 0;
	for (ClusterNode *node: ClusterManager::getClusterNodes()) {
		if (node != ClusterManager::getCurrentClusterNode()) {
			otherAlloc += node->getCurrentAllocCores();
			numOffloaded += node->getNumOffloadedTasks();
		}
	}

	_utilizationFile
		<< timestamp << " "
		<< allocCores << " "                                           //  1: alloc: determined by local or global policy
		<< enabledCores << " "                                         //  2: enabled: owned-lent+borrowed
		<< totalBusyCores << " "                                       //  3: busy: averaged number of busy cores
		<< usefulBusyCores << " "                                      //  4: useful-busy: averaged number of cores executing tasks
		<< ClusterMetrics::getNumReadyTasks() << " "                   //  5: localtasks: num. stealable or immovable ready tasks this instance
		<< ClusterManager::getTotalReadyTasks() << " "                 //  6: totaltasks: num. stealable ready tasks all instances this apprank
		<< ClusterMetrics::getTotalBusyCoresCurrentApprank() << " "    //  7: apprankbusy: sum of "busy" all instances this apprank
		<< ClusterMetrics::getNumImmovableTasks() << " "               //  8: immovable: num. immovable ready tasks (in local scheduler)
		<< "-1 "                                                       //  9: unused
		<< "-1 "                                                       // 10: unused
		<< numCpusOwned << " "                                         // 11: owned: number of owned CPUs
		<< numLentOwned << " "                                         // 12: lent: number of lent CPUs
		<< numBorrowed << " "                                          // 13: borrowed: number of borrowed CPUs
		<< ClusterHybridManager::getBusyOtherInstancesSameNode() << " "// 14:
		<< otherAlloc << " "                                           // 15:
		<< numOffloaded << "\n";                                       // 16:

	_utilizationFile.flush();
}

void ClusterHybridInterfaceFile::updateDROM(bool isGlobal)
{
	int ncpus = CPUManager::getTotalCPUs();
	int npids;
	int pidlist[32];
	int mypid = getpid();
	DLB_DROM_GetPidList(pidlist, &npids, 32);

	// Reduce our ownership if possible
	// This is our PID
	cpu_set_t mymask;
	DLBCPUActivation::getMyProcessMask(mymask);
	int numCpusOwnedOrGiving = DLBCPUActivation::getCurrentOwnedOrGivingCPUs();
	int numCpusOwned = DLBCPUActivation::getCurrentOwnedCPUs();
	int numCpusAlloced = ClusterManager::getCurrentClusterNode()->getCurrentAllocCores();

	if (!isGlobal) {
		// Local policy: scale by total demand
		updateTotalsThisNode();
	}

	bool setMyMask = false;

	if (numCpusOwnedOrGiving > numCpusAlloced) {
		int cpusToRelease = numCpusOwnedOrGiving - numCpusAlloced;
		for (int k=0; k<ncpus; k++) {
			CPU *cpu = CPUManager::getCPU(k);
			if (cpu->getActivationStatus() == CPU::giving_status) {
				// This CPU has volunteered to be given away

				// Change the status since the giving will be successful
				bool successfulStatus = DLBCPUActivation::changeStatusAndUpdateMetrics(cpu, CPU::giving_status, CPU::returned_status);
				if (successfulStatus) {
					setMyMask = true;
					CPU_CLR(k, &mymask);
					cpusToRelease --;
					if (cpusToRelease == 0) {
						break;
					}
				} else {
					// Doesn't matter: the giving may have been cancelled
				}
			}
		}

		if (cpusToRelease > 0) {
			for (int k=0; k<ncpus; k++) {
				CPU *cpu = CPUManager::getCPU(k);
				if (cpu->getActivationStatus() == CPU::lent_status) {

					bool successfulStatus = DLBCPUActivation::changeStatusAndUpdateMetrics(cpu, CPU::lent_status, CPU::returned_status);
					if (successfulStatus) {
						setMyMask = true;
						CPU_CLR(k, &mymask);
						cpusToRelease --;
						if (cpusToRelease == 0) {
							break;
						}
					}
				}
			}
		}

	} else if (numCpusOwned < numCpusAlloced) {

		// Try to find unused cores
		cpu_set_t claimed_mask;
		CPU_ZERO(&claimed_mask);

		for (int j=0; j<npids; j++) {
			cpu_set_t mask2;
			__attribute__((unused)) int ret = DLB_DROM_GetProcessMask(pidlist[j], &mask2, (dlb_drom_flags_t)0);
			assert(ret == DLB_SUCCESS);
			CPU_OR(&claimed_mask, &claimed_mask, &mask2);
		}
		// Try to claim sufficient unused cores
		int cpusToClaim = numCpusAlloced - numCpusOwned;
		for (int k=ncpus-1; k>=0; k--) {
			if (!CPU_ISSET(k, &claimed_mask)) {
				setMyMask = true;
				CPU_SET(k, &mymask);
				cpusToClaim --;
				if (cpusToClaim == 0) {
					break;
				}
			}
		}
	}

	if (setMyMask) {
		int ret = DLB_DROM_SetProcessMask(mypid, &mymask, (dlb_drom_flags_t)0);
		if (ret != DLB_SUCCESS && ret != DLB_ERR_TIMEOUT) {
			std::cout << "Unexpected return value from DLB_DROM_SetProcessMask: " << ret << "\n";
		}
		assert (ret == DLB_SUCCESS || ret == DLB_ERR_TIMEOUT);
	}

	// Now check if any CPUs are in giving_status (there may be due to a race condition)
	for (int k=0; k<ncpus; k++) {
		CPU *cpu = CPUManager::getCPU(k);
		if (cpu->getActivationStatus() == CPU::giving_status) {
			// This CPU has volunteered to be given away; put it in the lent state
			// then call the callback as if it was reclaimed
			bool successfulStatus = DLBCPUActivation::changeStatusAndUpdateMetrics(cpu, CPU::giving_status, CPU::lent_status);
			if (successfulStatus) {
				DLBCPUActivation::dlbEnableCallback(k, nullptr);
			}
		}
	}

	DLBCPUActivation::pollDROM(setMyMask);
	// DLBCPUActivation::checkCPUstates("after pollDROM", setMyMask);
}

#ifndef NDEBUG
void ClusterHybridInterfaceFile::checkNoDROM()
{
	int ncpus = CPUManager::getTotalCPUs();
	for (int k=0; k<ncpus; k++) {
		CPU *cpu = CPUManager::getCPU(k);
		assert (cpu->getActivationStatus() != CPU::giving_status);
	}
}
#endif

//! Called by polling service
void ClusterHybridInterfaceFile::poll()
{
	if (!ClusterHybridManager::getHybridInterfaceFileInitialized()) {
		/* Do not mess around with CPU ownership until after the CPU Manager has initialized */
		return;
	}
	struct timespec t;
	readTime(&t);
	float elapsedTime = (t.tv_sec - _prevTime.tv_sec)
					   + (t.tv_nsec - _prevTime.tv_nsec) / 1000000000.0;
	if (elapsedTime >= 0.5) {

		// DLBCPUActivation::checkCPUstates("start of polling service", false);

		_prevTime = t;
		bool changed = false;

		/*
		 * Send the utilization of this node (useful to see totatltasks from before it is updated).
		 */
		float timestamp;
		float usefulBusyCores;
		assert(Monitoring::runtimeStateIsEnabled());
		float totalBusyCores = RuntimeStateMonitor::readAndClear(timestamp, usefulBusyCores);
		Instrument::emitClusterEvent(Instrument::ClusterEventType::UsefulBusyCores, usefulBusyCores);
		Instrument::emitClusterEvent(Instrument::ClusterEventType::BusyCores, totalBusyCores);
		ClusterHybridInterfaceFile::appendUtilization(timestamp, totalBusyCores, usefulBusyCores);

		/*
		 * Update numbers from the other nodes
		 */
		int isGlobal = ClusterHybridManager::getHybridPolicy() == ClusterHybridPolicy::Global;
		if (isGlobal) {
			changed |= updateAllocFileGlobal();
		}
		changed |= ClusterHybridInterfaceFile::updateNumbersOfCores(!isGlobal, totalBusyCores);

		/*
		 * Redistribute the Dmallocs if necessary
		 */
		if (changed) {
			ClusterMemoryManagement::redistributeDmallocs(ClusterManager::clusterSize());
		}

		if (DLBCPUManager::getDromEnabled()) {
			updateDROM(isGlobal);
		} else {
#ifndef NDEBUG
			checkNoDROM();
#endif
		}
	}
}

ClusterHybridInterfaceFile::~ClusterHybridInterfaceFile()
{
	if (_utilizationFile.is_open()) {
		_utilizationFile << "DONE\n";
		_utilizationFile.close();
	}
}
