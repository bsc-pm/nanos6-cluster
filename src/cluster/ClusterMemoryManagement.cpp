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

ClusterMemoryManagement ClusterMemoryManagement::_singleton;

// Register a dmalloc by putting it on the list and registering the allocation with the directory.
void ClusterMemoryManagement::registerDmalloc(
	const DmallocDataInfo *dmallocDataInfo, Task *task, size_t clusterSize
) {
	DmallocDataInfo *ptr = (DmallocDataInfo *) malloc(dmallocDataInfo->getSize());
	assert(ptr != nullptr);

	memcpy(ptr, dmallocDataInfo, dmallocDataInfo->getSize());

	// Take write lock on the directory, which also protects _dmallocs.
	Directory::writeLock();
	// Construct a new DmallocInfo and place at the beginning of the _dmallocs list. Note that the
	// dimensions array, if not null, will be deep copied by the DmallocInfo constructor.
	_dmallocs.emplace_back(ptr);

	// Now register the allocation with the directory.
	ClusterDirectory::registerAllocation(dmallocDataInfo, task, clusterSize);

	Directory::writeUnlock();
}

// Unregister a dmalloc by removing it from the list and unregistering the allocation with the
// directory. Return true if successful.
bool ClusterMemoryManagement::unregisterDmalloc(DataAccessRegion const &region)
{
	void *startAddress = region.getStartAddress();

	// Take write lock on the directory, which also protects _dmallocs.
	Directory::writeLock();
	for (std::list<DmallocDataInfo *>::iterator it=_dmallocs.begin();
		 it != _dmallocs.end();
		 it++
	) {

		DmallocDataInfo *info = *it;

		if (info->_region.getStartAddress() == startAddress) {
			assert(info->_region.getSize() == region.getSize());
			ClusterDirectory::unregisterAllocation(region);    // Unregister from directory.
			free(info);
			_dmallocs.erase(it);                               // Remove from the list of Dmallocs
			Directory::writeUnlock();
			return true;
		} else {
			// The regions are not supposed to intersect
			assert(info->_region.intersect(region).empty());
		}
	}
	Directory::writeUnlock();
	return false;
}

// Redistribute the registered Dmallocs in the directory according to any updated policies.
void ClusterMemoryManagement::redistributeDmallocs(size_t newsize)
{
	// Take write lock on the directory, which also protects _dmallocs.
	Directory::writeLock();
	for (DmallocDataInfo *dmalloc : _singleton._dmallocs) {
		ClusterDirectory::unregisterAllocation(dmalloc->_region);
		ClusterDirectory::registerAllocation(dmalloc, nullptr, newsize);
	}
	Directory::writeUnlock();
}


void ClusterMemoryManagement::handleDmallocMessage(const MessageDmalloc *msg, Task *task)
{
	for (size_t i = 0; i < msg->getContent()->_ndmallocs; ++i) {

		const DmallocDataInfo *dataInfo = msg->getContent()->getData(i);

		assert(dataInfo->_region.getStartAddress() != nullptr);
		assert(dataInfo->_region.getSize() > 0);
		assert(dataInfo->_clusterSize > 0);

		//! Register region in the home node map This call adds the region to the list of Dmallocs
		//! that are automatically rebalanced and registers it with the cluster directory. Note that
		//! it is only registered in cluster mode.
		ClusterMemoryManagement::_singleton.registerDmalloc(dataInfo, task, dataInfo->_clusterSize);
	}

	//! Synchronize across all nodes
	ClusterManager::synchronizeAll();
}


void ClusterMemoryManagement::handleDfreeMessage(const MessageDfree *msg)
{
	assert(ClusterManager::inClusterMode());
	//! TODO: We need to fix the way we allocate distributed memory so that we do allocate it
	//! from the MemoryAllocator instead of the VirtualMemoryManagement layer, which is what we
	//! do now. The VirtualMemoryManagement layer does not allow (at the moment) deallocation of
	//! memory, so for now we do not free distributed memory
	const DataAccessRegion &distributedRegion = msg->getRegion();

	//! Unregister region from the home node map This call removes the region from the list of
	//! Dmallocs that are automatically rebalanced and unregisters it from the cluster
	//! directory. NOTE: that it should only be unregistered in cluster mode.
	const bool ok = ClusterMemoryManagement::_singleton.unregisterDmalloc(distributedRegion);
	FatalErrorHandler::failIf(!ok, "dfree an invalid region ", distributedRegion);

	ClusterManager::synchronizeAll();
}


void *ClusterMemoryManagement::dmalloc(
	size_t size,
	nanos6_data_distribution_t policy,
	size_t numDimensions,
	size_t *dimensions
) {
	//! We allocate distributed memory only on the master node, so that we serialize the
	//! allocations across all cluster nodes
	void *dptr = nullptr;
	if (ClusterManager::isMasterNode()) {
		dptr = VirtualMemoryManagement::allocDistrib(size);
		if (dptr == nullptr) {
			return nullptr;
		}
	}

	//! Get current task
	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	assert(currentThread != nullptr);
	Task *task = currentThread->getTask();

	const size_t clusterSize = ClusterManager::clusterSize();
	assert(clusterSize > 0);

	DataAccessRegion memoryRegion(dptr, size);

	//! Send a message to everyone else to let them know about the allocation
	MessageDmalloc msg(memoryRegion, clusterSize, policy, numDimensions, dimensions);
	assert(msg.getContent()->_ndmallocs == 1);
	DmallocDataInfo *data = msg.getContent()->getData(0);

	if (ClusterManager::inClusterMode()) {

		if (ClusterManager::isMasterNode()) {
			assert(data->_region.getStartAddress() != nullptr);
			ClusterManager::sendMessageToAll(&msg, true);
		} else {
			// Send a dmalloc message to master without pointer,
			assert(data->_region.getStartAddress() == nullptr);

			ClusterNode *master = ClusterManager::getMasterNode();
			ClusterManager::sendMessage(&msg, master, true);

			// wait for the address from master
			DataAccessRegion region(&dptr, sizeof(void *));
			ClusterManager::fetchDataRaw(region, master->getMemoryNode(), msg.getId(), true);
			assert(dptr != nullptr);
			data->_region = DataAccessRegion(dptr, data->_region.getSize());
		}

	} else {
		//! We are not in cluster mode, so no need to send or receive any messages
		assert(ClusterManager::isMasterNode());
	}
	ClusterMemoryManagement::handleDmallocMessage(&msg, task);

	return dptr;
}

void ClusterMemoryManagement::dfree(void *ptr, size_t size)
{
	assert(ptr != nullptr);
	assert(size > 0);

	//! Unregister region from the DataAccesses list of the Task
	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	assert(currentThread != nullptr);
	Task *currentTask = currentThread->getTask();

	DataAccessRegion distributedRegion(ptr, size);
	DataAccessRegistration::unregisterLocalAccess(currentTask, distributedRegion, /* isStack */ false);

	if (ClusterManager::inClusterMode()) {
		MessageDfree msg(distributedRegion);

		//! Here we should deallocate the memory once we fix the memory allocator API
		ClusterManager::sendMessageToAll(&msg, true);

		ClusterMemoryManagement::handleDfreeMessage(&msg);
	}
}

void *ClusterMemoryManagement::lmalloc(size_t size)
{
	// round the requested size to the cache line size to prevent
	// defragmenting the cache line resulting in unnecessarily
	// excessive number of accesses, and in turn update operations.
	size_t cacheLineSize = HardwareInfo::getCacheLineSize();
	size_t roundedSize = (size + cacheLineSize - 1) & ~(cacheLineSize - 1);

	// Register the lmalloc in the task's dependency system.  This is needed for taskwait
	// noflush, as a place to put the location information. Ideally we should register the whole
	// local region all at once. At the moment the lmalloc and lfree have to be in the same
	// task, which is a bit restrictive.
	void *lptr = MemoryAllocator::alloc(roundedSize);
	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	Task *task = currentThread->getTask();
	DataAccessRegion allocatedRegion(lptr, roundedSize);
	DataAccessRegistration::registerLocalAccess(
		task,
		allocatedRegion,
		ClusterManager::getCurrentMemoryNode(),
		/* isStack */ false
	);
	return lptr;
}

void ClusterMemoryManagement::lfree(void *ptr, size_t size)
{
	size_t cacheLineSize = HardwareInfo::getCacheLineSize();
	size_t roundedSize = (size + cacheLineSize - 1) & ~(cacheLineSize - 1);
	DataAccessRegion allocatedRegion(ptr, roundedSize);
	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	Task *task = currentThread->getTask();
	DataAccessRegistration::unregisterLocalAccess(task, allocatedRegion, /* isStack */ false);
	MemoryAllocator::free(ptr, roundedSize);
}
