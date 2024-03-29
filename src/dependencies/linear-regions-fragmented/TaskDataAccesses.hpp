/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef TASK_DATA_ACCESSES_HPP
#define TASK_DATA_ACCESSES_HPP

#include <atomic>
#include <bitset>
#include <cassert>
#include <mutex>
#include <random>

#include "BottomMapEntry.hpp"
#include "IntrusiveLinearRegionMap.hpp"
#include "IntrusiveLinearRegionMapImplementation.hpp"
#include "TaskDataAccessLinkingArtifacts.hpp"
#include "TaskDataAccessLinkingArtifactsImplementation.hpp"
#include "TaskDataAccessesInfo.hpp"
// #include "lowlevel/PaddedTicketSpinLock.hpp"
#include "lowlevel/RWSpinLock.hpp"


struct DataAccess;
class Task;

struct TaskDataAccesses
{
	typedef RWSpinLock spinlock_t;

	typedef IntrusiveLinearRegionMap<
		DataAccess,
		boost::intrusive::function_hook< TaskDataAccessLinkingArtifacts >
	> accesses_t;
	typedef IntrusiveLinearRegionMap<
		DataAccess,
		boost::intrusive::function_hook< TaskDataAccessLinkingArtifacts >
	> access_fragments_t;
	typedef IntrusiveLinearRegionMap<
		DataAccess,
		boost::intrusive::function_hook< TaskDataAccessLinkingArtifacts >
	> taskwait_fragments_t;
	typedef IntrusiveLinearRegionMap<
		BottomMapEntry,
		boost::intrusive::function_hook< BottomMapEntryLinkingArtifacts >
	> subaccess_bottom_map_t;

	spinlock_t _lock;
	accesses_t _accesses;
	access_fragments_t _accessFragments;
	taskwait_fragments_t _taskwaitFragments;
	subaccess_bottom_map_t _subaccessBottomMap;

	std::atomic<int> _removalBlockers;
	std::atomic<int> _liveTaskwaitFragmentCount;
	size_t _totalCommutativeBytes;

#ifndef NDEBUG
	enum flag_bits {
		HAS_BEEN_DELETED_BIT = 0,
		TOTAL_FLAG_BITS
	};
	typedef std::bitset<TOTAL_FLAG_BITS> flags_t;

	flags_t _flags = flags_t();

	bool hasBeenDeleted() const
	{
		return _flags[HAS_BEEN_DELETED_BIT];
	}

	flags_t::reference hasBeenDeleted()
	{
		return _flags[HAS_BEEN_DELETED_BIT];
	}
#endif

	TaskDataAccesses(__attribute__((unused)) TaskDataAccessesInfo taskAccessInfo)
		: _lock(),
		_accesses(),
		_accessFragments(),
		_taskwaitFragments(),
		_subaccessBottomMap(),
		_removalBlockers(0),
		_liveTaskwaitFragmentCount(0),
		_totalCommutativeBytes(0)
	{
	}

	~TaskDataAccesses();

	TaskDataAccesses(TaskDataAccesses const &other) = delete;

	inline size_t getAdditionalMemorySize() const
	{
		return 0;
	}

	inline size_t getTotalDataSize() const
	{
		return 0;
	}

	uint64_t computeNUMAAffinity(ComputePlace *);
};


#endif // TASK_DATA_ACCESSES_HPP
