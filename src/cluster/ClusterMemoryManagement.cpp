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

	struct DmallocInfo {

	   DataAccessRegion _region;
	   nanos6_data_distribution_t _policy;
	   size_t _nrDimensions;
	   size_t *_dimensions;

	   DmallocInfo(DataAccessRegion region,
							   nanos6_data_distribution_t policy,
							   size_t nrDimensions,
							   size_t *dimensions) :
							   _region(region), _policy(policy), _nrDimensions(nrDimensions)
	   {
			   if (dimensions) {
					   _dimensions = new size_t[nrDimensions];
					   memcpy(_dimensions, dimensions, nrDimensions * sizeof(size_t));
			   } else {
					   _dimensions = nullptr;
			   }
	   }
	   ~DmallocInfo()
	   {
			   if (_dimensions) {
					   delete[] _dimensions;
			   }
	   }
	};

	// List of all registered dmallocs
	std::list<DmallocInfo> _dmallocs;

	// Register a dmalloc by putting it on the list and registering the allocation with
	// the directory.
	void registerDmalloc(
		DataAccessRegion const &region,
		nanos6_data_distribution_t policy,
		size_t nrDimensions,
		size_t *dimensions,
		Task *task)
	{
		/*
		 * Take write lock on the directory, which also protects _dmallocs.
		 */
		Directory::writeLock();
		/*
		 * Construct a new DmallocInfo and place at the beginning of the _dmallocs
		 * list. Note that the dimensions array, if not null, will be deep copied
		 * by the DmallocInfo constructor.
		 */
		_dmallocs.emplace_back(region, policy, nrDimensions, dimensions);

		/*
		 * Now register the allocation with the directory.
		 */
		ClusterDirectory::registerAllocation(region,
											 policy,
											 nrDimensions,
											 dimensions,
											 task);
		Directory::writeUnlock();
	}

	// Unregister a dmalloc by removing it from the list and unregistering
	// the allocation with the directory. Return true if successful.
	bool unregisterDmalloc(DataAccessRegion const &region)
	{
		void *startAddress = region.getStartAddress();
		/*
		 * Take write lock on the directory, which also protects _dmallocs.
		 */
		Directory::writeLock();
		for(std::list<DmallocInfo>::iterator it=_dmallocs.begin();
			it!= _dmallocs.end();
			it++) {
			DmallocInfo &dmalloc = (*it);
			if (dmalloc._region.getStartAddress() == startAddress) {
				assert(dmalloc._region.getSize() == region.getSize());
				/*
				 * Remove from the list of Dmallocs
				 */
				_dmallocs.erase(it);
				Directory::writeUnlock();
				return true;
			}
		}
		Directory::writeUnlock();
		return false;
	}

	// Redistribute the registered Dmallocs in the directory according to
	// any updated policies.
	void redistributeDmallocs(void)
	{
		/*
		 * Take write lock on the directory, which also protects _dmallocs.
		 */
		Directory::writeLock();
		for(DmallocInfo &dmalloc : _dmallocs)
		{
			// std::cout << nanos6_get_cluster_node_id() << " redistribute dmalloc "
			// 		  << dmalloc._region.getStartAddress() << ":" << dmalloc._region.getSize() << "\n";

			ClusterDirectory::unregisterAllocation(dmalloc._region);
			ClusterDirectory::registerAllocation(dmalloc._region, dmalloc._policy, dmalloc._nrDimensions, dmalloc._dimensions, nullptr);
		}
		Directory::writeUnlock();
	}


	void handle_dmalloc_message(MessageDmalloc *msg, Task *task)
	{
		void *dptr = msg->getPointer();
		size_t size = msg->getAllocationSize();

		assert(dptr != nullptr);
		assert(size > 0);

		//! Register the newly allocated region with the Directory of home nodes
		DataAccessRegion allocatedRegion(dptr, size);

		//! Register region in the home node map
		//! This call adds the region to the list of Dmallocs that
		//! are automatically rebalanced and registers it with the cluster
		//! directory. Note that it is only registered in cluster mode.
		ClusterMemoryManagement::registerDmalloc(
			allocatedRegion,
			msg->getDistributionPolicy(),
			msg->getDimensionsSize(),
			msg->getDimensions(),
			task);

		//! Synchronize across all nodes
		ClusterManager::synchronizeAll();
	}


	void *dmalloc(
		size_t size,
		nanos6_data_distribution_t policy,
		size_t numDimensions,
		size_t *dimensions
	) {
		void *dptr = nullptr;

		//! Get current task
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		assert(currentThread != nullptr);
		Task *task = currentThread->getTask();

		ClusterNode *current = ClusterManager::getCurrentClusterNode();
		assert(current != nullptr);

		//! We allocate distributed memory only on the master node, so that we serialize the
		//! allocations across all cluster nodes
		if (ClusterManager::isMasterNode()) {
			dptr = VirtualMemoryManagement::allocDistrib(size);
			if (dptr == nullptr) {
				return nullptr;
			}
		}

		if (ClusterManager::inClusterMode()) {
			//! Send a message to everyone else to let them know about the allocation
			MessageDmalloc msg(current, dptr, size, policy, numDimensions, dimensions);

			if (ClusterManager::isMasterNode()) {
				assert(msg.getPointer() != nullptr);
				ClusterManager::sendMessageToAll(&msg, true);
			} else {
				// Send a dmalloc message to master without pointer,
				assert(msg.getPointer() == nullptr);

				ClusterNode *master = ClusterManager::getMasterNode();
				ClusterManager::sendMessage(&msg, master, true);

				DataAccessRegion region(&dptr, sizeof(void *));
				ClusterManager::fetchDataRaw(region, master->getMemoryNode(), msg.getId(), true);
				assert(dptr != nullptr);
				msg.setPointer(dptr);
			}

			handle_dmalloc_message(&msg, task);

		} else {
			//! If we are not in cluster mode we are done here
			assert(ClusterManager::isMasterNode());

			DataAccessRegion allocatedRegion(dptr, size);

			//! The home node of the new allocated region is the current node
			Directory::insert(allocatedRegion, ClusterManager::getCurrentMemoryNode());
			DataAccessRegistration::registerLocalAccess(
				task, allocatedRegion, ClusterManager::getCurrentMemoryNode(), /* isStack */ false
			);
		}

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

		//! We do not need to send any Messages here
		if (!ClusterManager::inClusterMode()) {
			//! Here we should deallocate the memory once we fix the memory allocator API
			return;
		}

		//! Unregister region from the home node map
		//! This call removes the region from the list of Dmallocs that
		//! are automatically rebalanced and unregisters it from the cluster
		//! directory. NOTE: that it should only be unregistered in cluster mode.
		bool ok = unregisterDmalloc(distributedRegion);
		FatalErrorHandler::failIf(!ok,
								  "dfree on invalid region ", distributedRegion);

		//! Send a message to everyone else to let them know about the deallocation
		const ClusterNode * const current = ClusterManager::getCurrentClusterNode();

		MessageDfree msg(current, ptr, size);
		ClusterManager::sendMessageToAll(&msg, true);
		ClusterManager::synchronizeAll();

		//! TODO: We need to fix the way we allocate distributed memory so that we do allocate it
		//! from the MemoryAllocator instead of the VirtualMemoryManagement layer, which is what we
		//! do now. The VirtualMemoryManagement layer does not allow (at the moment) deallocation of
		//! memory, so for now we do not free distributed memory
	}

	void *lmalloc(size_t size)
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

	void lfree(void *ptr, size_t size)
	{
		size_t cacheLineSize = HardwareInfo::getCacheLineSize();
		size_t roundedSize = (size + cacheLineSize - 1) & ~(cacheLineSize - 1);
		DataAccessRegion allocatedRegion(ptr, roundedSize);
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		Task *task = currentThread->getTask();
		DataAccessRegistration::unregisterLocalAccess(task, allocatedRegion);
		MemoryAllocator::free(ptr, roundedSize);
	}
}
