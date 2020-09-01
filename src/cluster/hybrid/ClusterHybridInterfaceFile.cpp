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

void ClusterHybridInterfaceFile::initialize(int externalRank)
{
	// std::cout << "ClusterHybridInterfaceFile::initialize\n";
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
			int ret = mkdir(".hybrid", 0777);
			FatalErrorHandler::failIf(
				ret != 0,
				"Cannot create .hybrid/ directory for hybrid cluster + DLB file interface"
				);
		}
	}
}


void ClusterHybridInterfaceFile::updateNumbersOfCores(void)
{
	// std::cout << "ClusterHybridInterfaceFile::updateNumbersOfCores\n";
}

void ClusterHybridInterfaceFile::sendUtilization(float ncores)
{
	// std::cout << "ClusterHybridInterfaceFile::sendUtilization " << ncores << "\n";
	(void)ncores;
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
		// std::cout << "ClusterHybridInterfaceFile::poll()\n";
	}
}
