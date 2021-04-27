/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include <iostream>
#include <fstream>
#include <sstream>
#include <string.h>
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "ClusterManager.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "ClusterHybridInterfaceFile.hpp"
#include "ClusterHybridManager.hpp"
#include "ClusterMemoryManagement.hpp"
#include <executors/threads/CPUManager.hpp>

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

void ClusterHybridInterfaceFile::initialize(int externalRank, int apprankNum)
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

bool ClusterHybridInterfaceFile::updateNumbersOfCores(void)
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
				}
			}
		}
        allocFile.close();
	}

	return changed;
}

//! Called by polling service
void ClusterHybridInterfaceFile::poll()
{
	struct timespec t;
	readTime(&t);
	float elapsedTime = (t.tv_sec - _prevTime.tv_sec)
					   + (t.tv_nsec - _prevTime.tv_nsec) / 1000000000.0;
	if (elapsedTime >= 0.5) {
		_prevTime = t;

		/*
		 * Update number of cores on each instance in the apprank
		 */
		bool changed = ClusterHybridInterfaceFile::updateNumbersOfCores();

		/*
		 * Redistribute the Dmallocs if necessary
		 */
		if (changed) {
			ClusterMemoryManagement::redistributeDmallocs(ClusterManager::clusterSize());
		}
	}
}
