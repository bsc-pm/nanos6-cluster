/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "ClusterManager.hpp"
#include "ClusterMemoryManagement.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "memory/directory/Directory.hpp"

#include <DataAccessRegistration.hpp>
#include <DistributionPolicy.hpp>
#include <MemoryAllocator.hpp>
#include <MessageDfree.hpp>
#include <MessageDmalloc.hpp>
#include <VirtualMemoryManagement.hpp>

namespace ClusterMemoryManagement {

	void *dmalloc(
		size_t size,
		nanos6_data_distribution_t policy,
		size_t numDimensions,
		size_t *dimensions
	) {
		void *dptr = nullptr;
		const bool isMaster = ClusterManager::isMasterNode();

		//! We allocate distributed memory only on the master node, so that we serialize the
		//! allocations across all cluster nodes
		if (isMaster) {
			dptr = VirtualMemoryManagement::allocDistrib(size);
			if (dptr == nullptr) {
				return nullptr;
			}
		}

		//! Get current task
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		assert(currentThread != nullptr);
		Task *task = currentThread->getTask();

		//! If we are not in cluster mode we are done here
		if (!ClusterManager::inClusterMode()) {
			assert(isMaster);

			DataAccessRegion allocatedRegion(dptr, size);

			//! The home node of the new allocated region is the current node
			Directory::insert(allocatedRegion, ClusterManager::getCurrentMemoryNode());
			DataAccessRegistration::registerLocalAccess(
				task, allocatedRegion, ClusterManager::getCurrentMemoryNode(), /* isStack */ false
			);

			return dptr;
		}

		//! Send a message to everyone else to let them know about the allocation
		ClusterNode *current = ClusterManager::getCurrentClusterNode();
		std::vector<ClusterNode *> const &world = ClusterManager::getClusterNodes();

		MessageDmalloc msg(current, numDimensions);
		msg.setAllocationSize(size);
		msg.setDistributionPolicy(policy);
		msg.setDimensions(dimensions);

		for (ClusterNode *node : world) {
			if (node == current) {
				continue;
			}

			ClusterManager::sendMessage(&msg, node, true);

			if (isMaster) {
				DataAccessRegion region(&dptr, sizeof(void *));
				ClusterManager::sendDataRaw(region, node->getMemoryNode(), msg.getId(), true);
			}
		}

		//! We are not the master node. The master node will send the allocated address
		if (!isMaster) {
			ClusterNode *master = ClusterManager::getMasterNode();
			DataAccessRegion region(&dptr, sizeof(void *));
			ClusterManager::fetchDataRaw(region, master->getMemoryNode(), msg.getId(), true);
		}

		//! Register the newly allocated region with the Directory of home nodes
		DataAccessRegion allocatedRegion(dptr, size);
		ClusterDirectory::registerAllocation(
			allocatedRegion, policy, numDimensions, dimensions, task
		);

		//! Register the new 'local' access


		//! Synchronize across all nodes
		ClusterManager::synchronizeAll();

		return dptr;
	}

	void dfree(void *ptr, size_t size)
	{
		assert(ptr != nullptr);
		assert(size > 0);

		DataAccessRegion distributedRegion(ptr, size);

		//! Unregister region from the DataAccesses list of the Task
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		assert(currentThread != nullptr);

		Task *currentTask = currentThread->getTask();
		DataAccessRegistration::unregisterLocalAccess(currentTask, distributedRegion);

		//! Unregister region from the home node map
		ClusterDirectory::unregisterAllocation(distributedRegion);

		//! We do not need to send any Messages here
		if (!ClusterManager::inClusterMode()) {
			//! Here we should deallocate the memory once we fix the memory allocator API
			return;
		}

		//! Send a message to everyone else to let them know about the deallocation
		const ClusterNode * const current = ClusterManager::getCurrentClusterNode();
		std::vector<ClusterNode *> const &world = ClusterManager::getClusterNodes();

		MessageDfree msg(current);
		msg.setAddress(ptr);
		msg.setSize(size);

		for (ClusterNode *node : world) {
			if (node == current) {
				continue;
			}

			ClusterManager::sendMessage(&msg, node, true);
		}

		ClusterManager::synchronizeAll();

		//! TODO: We need to fix the way we allocate distributed memory so that we do allocate it
		//! from the MemoryAllocator instead of the VirtualMemoryManagement layer, which is what we
		//! do now. The VirtualMemoryManagement layer does not allow (at the moment) deallocation of
		//! memory, so for now we do not free distributed memory
	}

	void *lmalloc(size_t size)
	{
		// Register the lmalloc in the task's dependency system.  This is needed for taskwait
		// noflush, as a place to put the location information. Ideally we should register the whole
		// local region all at once. At the moment the lmalloc and lfree have to be in the same
		// task, which is a bit restrictive.
		void *lptr = MemoryAllocator::alloc(size);
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		Task *task = currentThread->getTask();
		DataAccessRegion allocatedRegion(lptr, size);
		DataAccessRegistration::registerLocalAccess(
			task,
			allocatedRegion,
			ClusterManager::getCurrentMemoryNode(),
			/* isStack */ false
		);
		return lptr;
	}

	void lfree(void *ptr, size_t size)
	{
		DataAccessRegion allocatedRegion(ptr, size);
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		Task *task = currentThread->getTask();
		DataAccessRegistration::unregisterLocalAccess(task, allocatedRegion);
		MemoryAllocator::free(ptr, size);
	}
}
