/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include <boost/intrusive/parent_from_member.hpp>

#include "BottomMapEntry.hpp"
#include "DataAccess.hpp"
#include "ObjectAllocator.hpp"
#include "TaskDataAccesses.hpp"
#include "TaskDataAccessLinkingArtifacts.hpp"
#include "tasks/Task.hpp"
#include "memory/numa/NUMAManager.hpp"
#include "dependencies/DataTrackingSupport.hpp"
#include "hardware/hwinfo/HostInfo.hpp"

TaskDataAccesses::~TaskDataAccesses()
{
	assert(!hasBeenDeleted());
	assert(_removalBlockers == 0);

#ifndef NDEBUG
	Task *task = boost::intrusive::get_parent_from_member<Task>(this, &Task::_dataAccesses);
	assert(task != nullptr);
	assert(&task->getDataAccesses() == this);
#endif

	// We take the lock since the task may be marked for deletion while the lock is held
	std::lock_guard<spinlock_t> guard(_lock);
	_accesses.deleteAll(
		[&](DataAccess *access) {
			ObjectAllocator<DataAccess>::deleteObject(access);
		}
	);

	_subaccessBottomMap.deleteAll(
		[&](BottomMapEntry *bottomMapEntry) {
			ObjectAllocator<BottomMapEntry>::deleteObject(bottomMapEntry);
		}
	);

	_accessFragments.deleteAll(
		[&](DataAccess *fragment) {
			ObjectAllocator<DataAccess>::deleteObject(fragment);
		}
	);

	_taskwaitFragments.deleteAll(
		[&](DataAccess *fragment) {
			ObjectAllocator<DataAccess>::deleteObject(fragment);
		}
	);

#ifndef NDEBUG
	hasBeenDeleted() = true;
#endif
}

uint64_t TaskDataAccesses::computeNUMAAffinity(ComputePlace *)
{
	// NOTE: In ClusterMode, the computePlace is nullptr for the NodeNamespace
	// and for any tasks that become ready due to a MessageSatisfiability message
	// or other message processed by the polling service.
	if (!NUMAManager::isTrackingEnabled() ||
		!DataTrackingSupport::isNUMASchedulingEnabled())
	{
		return (uint64_t) -1;
	}

	size_t numNUMANodes = HardwareInfo::getMemoryPlaceCount(nanos6_host_device);
	size_t *bytesInNUMA = (size_t *)malloc(numNUMANodes * sizeof(size_t));
	assert(bytesInNUMA != nullptr);

	// Init bytesInNUMA to zero
	std::memset(bytesInNUMA, 0, numNUMANodes * sizeof(size_t));

	std::minstd_rand0 randomEngine;
	// std::minstd_rand0 &randomEngine = computePlace->getRandomEngine();
	size_t max = 0;
	uint64_t chosen = (uint64_t) -1;

	std::lock_guard<TaskDataAccesses::spinlock_t> guard(_lock);
	_accesses.processAll(
		[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
			DataAccess *dataAccess = &(*position);
			assert(dataAccess != nullptr);
			//! If the dataAccess is weak it is not really read/written, so no action required.
			if (!dataAccess->isWeak()) {
				DataAccessRegion region = dataAccess->getAccessRegion();

				// Get the home node for the region from the NUMA manager. If the
				// region has multiple parts on different NUMA nodes, we get the
				// NUMA node that has the largest number of bytes in the region.
				// This is always necessary because the linear region dependency
				// system does not pass the NUMA region through the dependencies,
				// which is what the discrete dependency system does. It should be
				// possible to do, but some care would be needed to deal with
				// merging and fragmentation of dependencies.
				uint8_t numaId = NUMAManager::getHomeNode(region.getStartAddress(), region.getSize());
				if (numaId != (uint8_t) -1) {
					assert(numaId < numNUMANodes);
					// Apply a bonus factor to RW accesses
					DataAccessType type = dataAccess->getType();
					bool rwAccess = (type != READ_ACCESS_TYPE) && (type != WRITE_ACCESS_TYPE);
					if (rwAccess) {
						bytesInNUMA[numaId] += dataAccess->getAccessRegion().getSize() * DataTrackingSupport::getRWBonusFactor();
					} else {
						bytesInNUMA[numaId] += dataAccess->getAccessRegion().getSize();
					}

					if (bytesInNUMA[numaId] > max) {
						max = bytesInNUMA[numaId];
						chosen = numaId;
					} else if (bytesInNUMA[numaId] == max && chosen != numaId) {
						// Random returns either 0 or 1. If 0, we keep the old max, if 1, we update it.
						std::uniform_int_distribution<unsigned int> unif(0, 1);
						unsigned int update = unif(randomEngine);
						if (update) {
							chosen = numaId;
						}
					}
				}
			}
			return true;
		});
	free(bytesInNUMA);

	return chosen;
}
