/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CPU_DEPENDENCY_DATA_HPP
#define CPU_DEPENDENCY_DATA_HPP


#include <atomic>
#include <bitset>
#include <boost/dynamic_bitset.hpp>
#include <limits.h>


#include "CommutativeScoreboard.hpp"
#include "DataAccessLink.hpp"
#include "DataAccessRegion.hpp"
#include "support/Containers.hpp"

#include <ExecutionStep.hpp>


#ifdef USE_CLUSTER
#include <SatisfiabilityInfo.hpp>  // Already includes WriteID.hpp
#endif // USE_CLUSTER

struct DataAccess;
class Task;
class ReductionInfo;
class MemoryPlace;


struct CPUDependencyData {
	struct UpdateOperation {
		DataAccessLink _target;
		DataAccessRegion _region;

		bool _makeReadSatisfied;
		bool _makeWriteSatisfied;
		bool _makeConcurrentSatisfied;
		bool _makeCommutativeSatisfied;
		bool _propagateSatisfiability;
		bool _setPropagateFromNamespace;
		MemoryPlace const *_location;
#ifdef USE_CLUSTER
		WriteID _writeID;
#endif // USE_CLUSTER
		bool _setReductionInfo; // Note: Both this and next field are required, as a null ReductionInfo can be propagated
		ReductionInfo *_reductionInfo;
		int _validNamespace;
		Task *_namespacePredecessor;

		boost::dynamic_bitset<> _reductionSlotSet;
		bool _previousIsCommutative;

		UpdateOperation()
			: _target(), _region(),
			_makeReadSatisfied(false), _makeWriteSatisfied(false),
			_makeConcurrentSatisfied(false), _makeCommutativeSatisfied(false),
			 _propagateSatisfiability(false),
			 _setPropagateFromNamespace(false),
			_location(nullptr),
#ifdef USE_CLUSTER
			_writeID(0),
#endif // USE_CLUSTER
			_setReductionInfo(false), _reductionInfo(nullptr),
			_validNamespace(-1),
			_namespacePredecessor(nullptr),
			_previousIsCommutative(false)
		{
		}

		UpdateOperation(DataAccessLink const &target, DataAccessRegion const &region)
			: _target(target), _region(region),
			_makeReadSatisfied(false), _makeWriteSatisfied(false),
			_makeConcurrentSatisfied(false), _makeCommutativeSatisfied(false),
			 _propagateSatisfiability(false),
			 _setPropagateFromNamespace(false),
			_location(nullptr),
#ifdef USE_CLUSTER
			_writeID(0),
#endif // USE_CLUSTER
			_setReductionInfo(false), _reductionInfo(nullptr),
			_validNamespace(-1),
			_namespacePredecessor(nullptr),
			_previousIsCommutative(false)
		{
		}

		bool empty() const
		{
			return !_makeReadSatisfied && !_makeWriteSatisfied
				&& !_makeConcurrentSatisfied && !_makeCommutativeSatisfied
				&& !_setPropagateFromNamespace
				&& !_setReductionInfo
				&& (_reductionSlotSet.size() == 0)
				&& !_namespacePredecessor;
		}
	};

	struct TaskAndRegion {
		Task *_task;
		DataAccessRegion _region;
		const MemoryPlace *_location;

		TaskAndRegion(Task *task, DataAccessRegion const &region, const MemoryPlace *location)
			: _task(task), _region(region), _location(location)
		{
		}

		bool operator<(TaskAndRegion const &other) const
		{
			if (_task < other._task) {
				return true;
			} else if (_task > other._task) {
				return false;
			} else {
				return (_region.getStartAddress() < other._region.getStartAddress());
			}
		}
		bool operator>(TaskAndRegion const &other) const
		{
			if (_task > other._task) {
				return true;
			} else if (_task < other._task) {
				return false;
			} else {
				return (_region.getStartAddress() > other._region.getStartAddress());
			}
		}
		bool operator==(TaskAndRegion const &other) const
		{
			return (_task == other._task) && (_region == other._region);
		}
		bool operator!=(TaskAndRegion const &other) const
		{
			return (_task == other._task) && (_region == other._region);
		}
	};

	typedef Container::list<UpdateOperation> delayed_operations_t;
	typedef Container::deque<Task *> satisfied_originator_list_t;
	typedef Container::deque<Task *> removable_task_list_t;
	typedef Container::deque<CommutativeScoreboard::entry_t *> acquired_commutative_scoreboard_entries_t;
	typedef Container::deque<TaskAndRegion> released_commutative_regions_t;
	typedef Container::deque<DataAccess *> satisfied_taskwait_accesses_t;

	//! Tasks whose accesses have been satisfied after ending a task
	satisfied_originator_list_t _satisfiedOriginators;
	satisfied_originator_list_t _satisfiedCommutativeOriginators;
	delayed_operations_t _delayedOperations;
	removable_task_list_t _removableTasks;
	acquired_commutative_scoreboard_entries_t _acquiredCommutativeScoreboardEntries;
	released_commutative_regions_t _releasedCommutativeRegions;
	satisfied_taskwait_accesses_t _completedTaskwaits;
#ifndef NDEBUG
	std::atomic<bool> _inUse;
#endif // NDEBUG

#ifdef USE_CLUSTER
	TaskOffloading::SatisfiabilityInfoMap _satisfiabilityMap; // Node's: list of satisfiabilities to send.
#endif // USE_CLUSTER

	CPUDependencyData()
		: _satisfiedOriginators(), _satisfiedCommutativeOriginators(),
		_delayedOperations(), _removableTasks(),
		_acquiredCommutativeScoreboardEntries(), _releasedCommutativeRegions(),
		_completedTaskwaits()
#ifndef NDEBUG
		, _inUse(false)
#endif // NDEBUG
#ifdef USE_CLUSTER
		, _satisfiabilityMap() // Node's: list of satisfiabilities to send.
#endif // USE_CLUSTER
	{
	}

	~CPUDependencyData()
	{
		assert(empty());
	}

	inline bool empty() const
	{
		return _satisfiedOriginators.empty() && _satisfiedCommutativeOriginators.empty()
			&& _delayedOperations.empty() && _removableTasks.empty()
			&& _acquiredCommutativeScoreboardEntries.empty()
			&& _completedTaskwaits.empty()
#ifdef USE_CLUSTER
			&& _satisfiabilityMap.empty()
#endif // USE_CLUSTER
			;
	}
};


#endif // CPU_DEPENDENCY_DATA_HPP
