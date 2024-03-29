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
#include "lowlevel/FatalErrorHandler.hpp"


extern "C" int nanos6_in_cluster_mode(void)
{
	return ClusterManager::inClusterMode();
}

// Compatibility in pure OmpSs-2@Cluster, "node" means irank
extern "C" int nanos6_is_master_node(void)
{
	return nanos6_is_master_irank();
}

// Compatibility: in pure OmpSs-2@Cluster, "node" means irank
extern "C" int nanos6_get_cluster_node_id(void)
{
	return nanos6_get_cluster_irank_id();
}

// Compatibility: in pure OmpSs-2@Cluster, "node" means irank
extern "C" int nanos6_get_num_cluster_nodes(void)
{
	return nanos6_get_num_cluster_iranks();
}

extern "C" int nanos6_is_master_irank(void)
{
	return ClusterManager::isMasterNode();
}

extern "C" int nanos6_get_cluster_physical_node_id(void)
{
	return ClusterManager::getPhysicalNodeNum();
}

extern "C" int nanos6_get_num_cluster_physical_nodes(void)
{
	return ClusterManager::getNumNodes();
}

extern "C" int nanos6_get_cluster_irank_id(void)
{
	return ClusterManager::getCurrentClusterNode()->getIndex();
}

extern "C" int nanos6_get_num_cluster_iranks(void)
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

extern "C" int nanos6_get_app_communicator(void *appcomm)
{
#ifndef USE_CLUSTER
	// If the runtime doesn't have support for cluster, then it hasn't been built with
	// MPI. So we cannot return an MPI communicator of any sort.
	FatalErrorHandler::fail(
		"nanos6_get_app_communicator() should only be called if Nanos6 is built with cluster support.");
#endif
	MPI_Comm *appcomm_ptr = (MPI_Comm *)appcomm;
	*appcomm_ptr = ClusterManager::getAppCommunicator();
	return 0;
}
