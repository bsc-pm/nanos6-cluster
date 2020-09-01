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

	/*
	 * Filenames for this apprank's core allocation
	 * (cannot call ClusterManager::getApprankNum() as this function is
	 * called during the initialization of ClusterManager)
	 */
	std::stringstream ss;
	ss << _directory << "/alloc" << apprankNum;
	_allocFileThisApprank = strdup(ss.str().c_str());
}


void ClusterHybridInterfaceFile::sendUtilization(float ncores)
{
	// std::cout << "ClusterHybridInterfaceFile::sendUtilization " << ncores << "\n";
	(void)ncores;
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
	if (elapsedTime >= 2.0) {
		_prevTime = t;

		bool changed = ClusterHybridInterfaceFile::updateNumbersOfCores();
		if (changed) {
			ClusterMemoryManagement::redistributeDmallocs();
		}
	}
}
