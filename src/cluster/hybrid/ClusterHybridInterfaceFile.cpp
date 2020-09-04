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
#include "ClusterStats.hpp"

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
		int nodeNum,
		int indexThisNode)
{
	/*
	 * External rank 0 clears or creates the .hybrid/ directory (NOTE: cannot
	 * call ClusterManager::getExternalRank() as this function is called during
	 * the initialization of ClusterManager)
	 */
	if (externalRank  == 0) {
		struct stat sb;
		if (stat(_directory, &sb) == 0 && S_ISDIR(sb.st_mode)) {
			// .hybrid/ directory already exists: clear everything inside it
			DIR *dirStream = opendir(_directory);
			struct dirent *file;
			while ( (file = readdir(dirStream)) != nullptr) {
				std::stringstream ss;
				ss << _directory << "/" << file->d_name;
				remove(ss.str().c_str());
			}
			closedir(dirStream);
		} else {
			// Create empty .hybrid directory
			int ret = mkdir(_directory, 0777);
			FatalErrorHandler::failIf(
					ret != 0,
					"Cannot create .hybrid/ directory for hybrid cluster + DLB file interface"
					);
		}
	}

	MPI_Barrier(MPI_COMM_WORLD);

	/*
	 * Now create our map file
	 */
	std::stringstream ss0;
	ss0 << _directory << "/map" << externalRank;
	std::ofstream mapFile(ss0.str().c_str());
	mapFile << "externalRank " << externalRank << "\n"
		<< "apprankNum " << apprankNum << "\n"
		<< "internalRank " << internalRank << "\n"
		<< "nodeNum " << nodeNum << "\n"
		<< "indexThisNode " << indexThisNode << "\n";
	mapFile.close();

	/*
	 * Filenames for this apprank's core allocation
	 * (cannot call ClusterManager::getApprankNum() as this function is
	 * called during the initialization of ClusterManager)
	 */
	std::stringstream ss1;
	ss1 << _directory << "/alloc" << apprankNum;
	_allocFileThisApprank = strdup(ss1.str().c_str());

	/*
	 * Open file for this instance's utilization
	 */
	std::stringstream ss2;
	ss2 << _directory << "/utilization" << externalRank;
	std::string s2 = ss2.str();
	const char *utilizationFilename = s2.c_str();
	_utilizationFile.open(utilizationFilename);
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

void ClusterHybridInterfaceFile::appendUtilization(float timestamp, float busy_cores)
{
	int _enabledCores = ClusterHybridManager::countEnabledCPUs();
	_utilizationFile << timestamp << " "
	                 << ClusterManager::getCurrentClusterNode()->getCurrentAllocCores() << " "
					 << _enabledCores << " "
					 << busy_cores << "\n";
	_utilizationFile.flush();
}

//! Called by polling service
void ClusterHybridInterfaceFile::poll()
{
	struct timespec t;
	readTime(&t);
	float elapsedTime = (t.tv_sec - _prevTime.tv_sec)
					   + (t.tv_nsec - _prevTime.tv_nsec) / 1000000000.0;
	if (elapsedTime >= 2.0) {
		_prevTime = t;

		/*
		 * Update number of cores on each instance in the apprank
		 */
		bool changed = ClusterHybridInterfaceFile::updateNumbersOfCores();

		/*
		 * Redistribute the Dmallocs if necessary
		 */
		if (changed) {
			ClusterMemoryManagement::redistributeDmallocs();
		}

		if (ClusterManager::inClusterMode()) {
			/*
			 * Send the utilization of this node
			 */
			float timestamp;
			float usefulBusyCores;
			float busy_cores = ClusterStats::readAndClearCoresBusy(timestamp, usefulBusyCores);
			ClusterHybridInterfaceFile::appendUtilization(timestamp, busy_cores);
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
