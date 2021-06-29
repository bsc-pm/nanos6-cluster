/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/


#include "MemoryPlace.hpp"

#include "cluster/ClusterManager.hpp"
#include "memory/directory/Directory.hpp"

bool MemoryPlace::isDirectoryMemoryPlace() const
{
	return Directory::isDirectoryMemoryPlace(this);
}

bool MemoryPlace::isClusterLocalMemoryPlace() const
{
	return (!isDirectoryMemoryPlace()
		&& (_type != nanos6_cluster_device || this == ClusterManager::getCurrentMemoryNode()));
}
