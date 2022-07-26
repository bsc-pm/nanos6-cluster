/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include "FatalErrorHandler.hpp"
#include <ClusterManager.hpp>

SpinLock FatalErrorHandler::_errorLock;
SpinLock FatalErrorHandler::_infoLock;

[[ noreturn ]] void FatalErrorHandler::nanos6Abort()
{
#ifdef USE_CLUSTER
	ClusterManager::tryAbortAll();
#endif // USE_CLUSTER

#ifndef NDEBUG
	abort();
#else // NDEBUG
	exit(EXIT_FAILURE);
#endif // NDEBUG
}

std::string FatalErrorHandler::getErrorPrefix()
{
#ifdef USE_CLUSTER
	if (ClusterManager::isInitialized() && ClusterManager::inClusterMode()) {
		std::stringstream ss;
		ss << "Node:" << ClusterManager::getCurrentClusterNode()->getIndex() << ": ";
		return ss.str();
	}
#endif // USE_CLUSTER
	return "";
}
