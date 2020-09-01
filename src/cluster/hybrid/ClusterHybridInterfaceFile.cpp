/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include <iostream>
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "ClusterManager.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "ClusterHybridInterfaceFile.hpp"

ClusterHybridInterfaceFile::ClusterHybridInterfaceFile()
{
	ConfigVariable<std::string> clusterHybridDirectory("cluster.hybrid.directory");
	std::string s = clusterHybridDirectory.getValue();
	_directory = strdup(s.c_str());
	// Get current time for polling service
	readTime(&_prevTime);
}

void ClusterHybridInterfaceFile::initialize(int externalRank, __attribute__((unused)) int apprankNum)
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
}


bool ClusterHybridInterfaceFile::updateNumbersOfCores(void)
{
	return false;
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
	}
}
