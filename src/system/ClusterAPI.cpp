/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#include <nanos6/cluster.h>

#include <ClusterManager.hpp>
#include <ClusterMemoryManagement.hpp>
#include <ClusterNode.hpp>
#include "tasks/Task.hpp"
#include "executors/threads/WorkerThread.hpp"


extern "C" int nanos6_in_cluster_mode(void)
{
	return ClusterManager::inClusterMode();
}

extern "C" int nanos6_is_master_node(void)
{
	return ClusterManager::isMasterNode();
}

extern "C" int nanos6_get_cluster_node_id(void)
{
	return ClusterManager::getCurrentClusterNode()->getIndex();
}

extern "C" int nanos6_get_num_cluster_nodes(void)
{
	return ClusterManager::clusterSize();
}

extern "C" int nanos6_get_namespace_is_enabled(void)
{
	// Invert this value.
	return ClusterManager::getDisableRemote() == false;
}


extern "C" void *nanos6_dmalloc(
	size_t size,
	nanos6_data_distribution_t policy,
	size_t num_dimensions,
	size_t *dimensions
) {
	if (size == 0) {
		return nullptr;
	}

	return ClusterMemoryManagement::dmalloc(size, policy, num_dimensions, dimensions);
}

extern "C" void nanos6_dfree(void *ptr, size_t size)
{
	ClusterMemoryManagement::dfree(ptr, size);
}

extern "C" void *nanos6_lmalloc(size_t size)
{
	if (size == 0) {
		return nullptr;
	}

	return ClusterMemoryManagement::lmalloc(size);
}

extern "C" void nanos6_lfree(void *ptr, size_t size)
{
	ClusterMemoryManagement::lfree(ptr, size);
}

extern "C" void nanos6_set_early_release(nanos6_early_release_t early_release)
{
	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	Task *task = currentThread->getTask();
	assert(task != nullptr);
	task->setEarlyRelease(early_release);
}

