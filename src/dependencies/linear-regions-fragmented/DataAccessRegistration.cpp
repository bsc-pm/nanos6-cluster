/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.
	
	Copyright (C) 2015-2017 Barcelona Supercomputing Center (BSC)
*/

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <cassert>
#include <deque>
#include <mutex>
#include <vector>

#include "BottomMapEntry.hpp"
#include "CPUDependencyData.hpp"
#include "DataAccess.hpp"
#include "DataAccessRegistration.hpp"

#include "executors/threads/TaskFinalization.hpp"
#include "executors/threads/ThreadManager.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "hardware/places/ComputePlace.hpp"
#include "scheduling/Scheduler.hpp"
#include "tasks/Task.hpp"

#include "TaskDataAccessesImplementation.hpp"

#include <InstrumentDependenciesByAccessLinks.hpp>
#include <InstrumentComputePlaceId.hpp>
#include <InstrumentLogMessage.hpp>
#include <InstrumentTaskId.hpp>


#pragma GCC visibility push(hidden)

namespace DataAccessRegistration {
	typedef CPUDependencyData::removable_task_list_t removable_task_list_t;
	
	
	typedef CPUDependencyData::UpdateOperation UpdateOperation;
	
	
	struct DataAccessStatusEffects {
		bool _isRegistered;
		bool _isSatisfied;
		bool _enforcesDependency;
		
		bool _hasNext;
		bool _propagatesReadSatisfiabilityToNext;
		bool _propagatesWriteSatisfiabilityToNext;
		bool _propagatesConcurrentSatisfiabilityToNext;
		reduction_type_and_operator_index_t _propagatesReductionSatisfiabilityToNext;
		bool _makesNextTopmost;
		bool _propagatesTopLevel;
		
		bool _propagatesReadSatisfiabilityToFragments;
		bool _propagatesWriteSatisfiabilityToFragments;
		bool _propagatesConcurrentSatisfiabilityToFragments;
		reduction_type_and_operator_index_t _propagatesReductionSatisfiabilityToFragments;
		
		bool _linksBottomMapAccessesToNextAndInhibitsPropagation;
		
		bool _isRemovable;
		
	public:
		DataAccessStatusEffects()
			: _isRegistered(false),
			_isSatisfied(false), _enforcesDependency(false),
			
			_hasNext(false),
			_propagatesReadSatisfiabilityToNext(false), _propagatesWriteSatisfiabilityToNext(false), _propagatesConcurrentSatisfiabilityToNext(false),
			_propagatesReductionSatisfiabilityToNext(no_reduction_type_and_operator),
			_makesNextTopmost(false),
			_propagatesTopLevel(false),
			
			_propagatesReadSatisfiabilityToFragments(false), _propagatesWriteSatisfiabilityToFragments(false), _propagatesConcurrentSatisfiabilityToFragments(false),
			_propagatesReductionSatisfiabilityToFragments(no_reduction_type_and_operator),
			
			_linksBottomMapAccessesToNextAndInhibitsPropagation(false),
			
			_isRemovable(false)
		{
		}
		
		DataAccessStatusEffects(DataAccess const *access)
		{
			_isRegistered = access->isRegistered();
			
			_isSatisfied = access->satisfied();
			_enforcesDependency = !access->isWeak() && !access->satisfied() && (access->getObjectType() == access_type);
			_hasNext = access->hasNext();
			
			// Propagation to fragments
			if (access->hasSubaccesses()) {
				_propagatesReadSatisfiabilityToFragments = access->readSatisfied();
				_propagatesWriteSatisfiabilityToFragments = access->writeSatisfied();
				_propagatesConcurrentSatisfiabilityToFragments = access->concurrentSatisfied();
				if (access->anyReductionSatisfied()) {
					_propagatesReductionSatisfiabilityToFragments = any_reduction_type_and_operator;
				} else if (access->matchingReductionSatisfied()) {
					_propagatesReductionSatisfiabilityToFragments = access->getReductionTypeAndOperatorIndex();
				} else {
					_propagatesReductionSatisfiabilityToFragments = no_reduction_type_and_operator;
				}
			} else {
				_propagatesReadSatisfiabilityToFragments = false;
				_propagatesWriteSatisfiabilityToFragments = false;
				_propagatesConcurrentSatisfiabilityToFragments = false;
				_propagatesReductionSatisfiabilityToFragments = no_reduction_type_and_operator;
			}
			
			// Propagation to next
			if (_hasNext) {
				assert(access->getObjectType() != taskwait_type);
				assert(access->getObjectType() != top_level_sink_type);
				
				if (access->hasSubaccesses()) {
					assert(access->getObjectType() == access_type);
					_propagatesReadSatisfiabilityToNext =
						access->readSatisfied() && access->canPropagateReadSatisfiability()
						&& (access->getType() == READ_ACCESS_TYPE);
					_propagatesWriteSatisfiabilityToNext = false; // Write satisfiability is propagated through the fragments
					_propagatesConcurrentSatisfiabilityToNext =
						access->canPropagateConcurrentSatisfiability()
						&& access->concurrentSatisfied() && (access->getType() == CONCURRENT_ACCESS_TYPE);
					
					if (
						!access->canPropagateAnyReductionSatisfiability()
						&& !access->canPropagateMatchingReductionSatisfiability()
					) {
						_propagatesReductionSatisfiabilityToNext = no_reduction_type_and_operator;
					} else if (
						access->canPropagateMatchingReductionSatisfiability()
						&& (access->matchingReductionSatisfied() || access->anyReductionSatisfied())
						&& (access->getType() == REDUCTION_ACCESS_TYPE)
					) {
						_propagatesReductionSatisfiabilityToNext = access->getReductionTypeAndOperatorIndex();
					} else {
						// Reduction satisfiability of non-reductions is propagated through the fragments
						_propagatesReductionSatisfiabilityToNext = no_reduction_type_and_operator;
					}
				} else if (
					(access->getObjectType() == fragment_type)
					|| (access->getObjectType() == taskwait_type)
					|| (access->getObjectType() == top_level_sink_type)
				) {
					_propagatesReadSatisfiabilityToNext =
						access->canPropagateReadSatisfiability()
						&& access->readSatisfied();
					_propagatesWriteSatisfiabilityToNext = access->writeSatisfied();
					_propagatesConcurrentSatisfiabilityToNext =
						access->canPropagateConcurrentSatisfiability()
						&& access->concurrentSatisfied();
					
					if (
						access->canPropagateAnyReductionSatisfiability()
						&& access->anyReductionSatisfied()
					) {
						_propagatesReductionSatisfiabilityToNext = any_reduction_type_and_operator;
					} else if (
						access->canPropagateMatchingReductionSatisfiability()
						&& access->matchingReductionSatisfied()
					) {
						_propagatesReductionSatisfiabilityToNext = access->getReductionTypeAndOperatorIndex();
					} else {
						_propagatesReductionSatisfiabilityToNext = no_reduction_type_and_operator;
					}
				} else {
					assert(access->getObjectType() == access_type);
					assert(!access->hasSubaccesses());
					
					// A regular access without subaccesses but with a next
					_propagatesReadSatisfiabilityToNext =
						access->canPropagateReadSatisfiability()
						&& access->readSatisfied()
						&& ((access->getType() == READ_ACCESS_TYPE) || access->complete());
					_propagatesWriteSatisfiabilityToNext =
						access->writeSatisfied() && access->complete();
					_propagatesConcurrentSatisfiabilityToNext =
						access->canPropagateConcurrentSatisfiability()
						&& access->concurrentSatisfied()
						&& (access->complete() || (access->getType() == CONCURRENT_ACCESS_TYPE));
					
					if (
						access->canPropagateAnyReductionSatisfiability()
						&& access->anyReductionSatisfied()
						&& access->complete()
					) {
						_propagatesReductionSatisfiabilityToNext = any_reduction_type_and_operator;
					} else if (
						access->canPropagateMatchingReductionSatisfiability()
						&& access->anyReductionSatisfied()
						&& (access->getType() == REDUCTION_ACCESS_TYPE)
					) {
						_propagatesReductionSatisfiabilityToNext = access->getReductionTypeAndOperatorIndex();
					} else if (
						access->canPropagateMatchingReductionSatisfiability()
						&& access->matchingReductionSatisfied()
						&& (access->getType() == REDUCTION_ACCESS_TYPE)
					) {
						_propagatesReductionSatisfiabilityToNext = access->getReductionTypeAndOperatorIndex();
					} else {
						_propagatesReductionSatisfiabilityToNext = no_reduction_type_and_operator;
					}
				}
			} else {
				assert(!access->hasNext());
				_propagatesReadSatisfiabilityToNext = false;
				_propagatesWriteSatisfiabilityToNext = false;
				_propagatesConcurrentSatisfiabilityToNext = false;
				_propagatesReductionSatisfiabilityToNext = no_reduction_type_and_operator;
			}
			
			_isRemovable = access->isTopmost()
				&& access->readSatisfied() && access->writeSatisfied()
				&& access->complete()
				&& (
					!access->isInBottomMap() || access->hasNext()
					|| (access->getObjectType() == taskwait_type)
					|| (access->getObjectType() == top_level_sink_type)
				);
			
			Task *domainParent;
			assert(access->getOriginator() != nullptr);
			if (access->getObjectType() == access_type) {
				domainParent = access->getOriginator()->getParent();
			} else {
				assert(
					(access->getObjectType() == fragment_type)
					|| (access->getObjectType() == taskwait_type)
					|| (access->getObjectType() == top_level_sink_type)
				);
				domainParent = access->getOriginator();
			}
			assert(domainParent != nullptr);
			
			if (_isRemovable && access->hasNext()) {
				Task *nextDomainParent;
				if (access->getNext()._objectType == access_type) {
					nextDomainParent = access->getNext()._task->getParent();
				} else {
					assert(
						(access->getNext()._objectType == fragment_type)
						|| (access->getNext()._objectType == taskwait_type)
						|| (access->getNext()._objectType == top_level_sink_type)
					);
					nextDomainParent = access->getNext()._task;
				}
				assert(nextDomainParent != nullptr);
				
				_makesNextTopmost = (domainParent == nextDomainParent);
			} else {
				_makesNextTopmost = false;
			}
			
			_propagatesTopLevel =
				access->isTopLevel()
				&& access->hasNext()
				&& (access->getOriginator()->getParent() == access->getNext()._task->getParent());
			
			// NOTE: Calculate inhibition from initial status
			_linksBottomMapAccessesToNextAndInhibitsPropagation =
				access->hasNext() && access->complete() && access->hasSubaccesses();
		}
	};
	
	
	struct BottomMapUpdateOperation {
		DataAccessRegion _region;
		
		bool _linkBottomMapAccessesToNext;
		
		bool _inhibitReadPropagation;
		bool _inhibitConcurrentPropagation;
		reduction_type_and_operator_index_t _inhibitReductionPropagation;
		
		DataAccessLink _next;
		
		BottomMapUpdateOperation()
			: _region(),
			_linkBottomMapAccessesToNext(false),
			_inhibitReadPropagation(false),
			_inhibitConcurrentPropagation(false),
			_inhibitReductionPropagation(no_reduction_type_and_operator),
			_next()
		{
		}
		
		BottomMapUpdateOperation(DataAccessRegion const &region)
			: _region(region),
			_linkBottomMapAccessesToNext(false),
			_inhibitReadPropagation(false),
			_inhibitConcurrentPropagation(false),
			_inhibitReductionPropagation(no_reduction_type_and_operator),
			_next()
		{
		}
		
		bool empty() const
		{
			return !_linkBottomMapAccessesToNext;
		}
	};
	
	
	// Forward declarations
	static inline void processBottomMapUpdate(
		BottomMapUpdateOperation &operation,
		TaskDataAccesses &accessStructures, Task *task,
		/* OUT */ CPUDependencyData &hpDependencyData
	);
	static inline void removeBottomMapTaskwaitOrTopLevelSink(
		DataAccess *access, TaskDataAccesses &accessStructures, __attribute__((unused)) Task *task 
	);
	static inline BottomMapEntry *fragmentBottomMapEntry(
		BottomMapEntry *bottomMapEntry, DataAccessRegion region,
		TaskDataAccesses &accessStructures, bool removeIntersection = false
	);
	static void handleRemovableTasks(
		/* inout */ CPUDependencyData::removable_task_list_t &removableTasks,
		ComputePlace *computePlace
	);
	
	
	static inline void handleDataAccessStatusChanges(
		DataAccessStatusEffects const &initialStatus,
		DataAccessStatusEffects const &updatedStatus,
		DataAccess *access, TaskDataAccesses &accessStructures, Task *task,
		/* OUT */ CPUDependencyData &hpDependencyData
	) {
		// Registration
		if (initialStatus._isRegistered != updatedStatus._isRegistered) {
			assert(!initialStatus._isRegistered);
			
			// Count the access
			if (!initialStatus._isRemovable) {
				if (accessStructures._removalBlockers == 0) {
					// The blocking count is decreased once all the accesses become removable
					task->increaseRemovalBlockingCount();
				}
				accessStructures._removalBlockers++;
				if (access->getObjectType() == taskwait_type) {
					accessStructures._liveTaskwaitFragmentCount++;
				}
			}
			
			if (updatedStatus._enforcesDependency) {
				task->increasePredecessors();
			}
		}
		
		if (!updatedStatus._isRegistered) {
			return;
		}
		
		// NOTE: After this point, all actions assume the access is registered
		
		// Satisfiability
		if (initialStatus._isSatisfied != updatedStatus._isSatisfied) {
			assert(!initialStatus._isSatisfied);
			Instrument::dataAccessBecomesSatisfied(
				access->getInstrumentationId(),
				true,
				task->getInstrumentationTaskId()
			);
		}
		
		// Link to Next
		if (initialStatus._hasNext != updatedStatus._hasNext) {
			assert(!initialStatus._hasNext);
			Instrument::linkedDataAccesses(
				access->getInstrumentationId(),
				access->getNext()._task->getInstrumentationTaskId(),
				(Instrument::access_object_type_t) access->getNext()._objectType,
				access->getAccessRegion(),
				/* direct */ true, /* unidirectional */ false
			);
		}
		
		// Dependency updates
		if (initialStatus._enforcesDependency != updatedStatus._enforcesDependency) {
			if (updatedStatus._enforcesDependency) {
				// A new access that enforces a dependency.
				// Already counted as part of the registration status change.
				assert(!initialStatus._isRegistered && updatedStatus._isRegistered);
			} else {
				// The access no longer enforces a dependency (has become satisified)
				if (task->decreasePredecessors()) {
					// The task becomes ready
					hpDependencyData._satisfiedOriginators.push_back(task);
				}
			}
		}
		
		// Propagation to Next
		if (access->hasNext()) {
			UpdateOperation updateOperation(access->getNext(), access->getAccessRegion());
			
			if (initialStatus._propagatesReadSatisfiabilityToNext != updatedStatus._propagatesReadSatisfiabilityToNext) {
				assert(!initialStatus._propagatesReadSatisfiabilityToNext);
				updateOperation._makeReadSatisfied = true;
			}
			
			if (initialStatus._propagatesWriteSatisfiabilityToNext != updatedStatus._propagatesWriteSatisfiabilityToNext) {
				assert(!initialStatus._propagatesWriteSatisfiabilityToNext);
				updateOperation._makeWriteSatisfied = true;
			}
			
			if (initialStatus._propagatesConcurrentSatisfiabilityToNext != updatedStatus._propagatesConcurrentSatisfiabilityToNext) {
				assert(!initialStatus._propagatesConcurrentSatisfiabilityToNext);
				updateOperation._makeConcurrentSatisfied = true;
			}
			
			if (initialStatus._propagatesReductionSatisfiabilityToNext != updatedStatus._propagatesReductionSatisfiabilityToNext) {
				assert(updatedStatus._propagatesReductionSatisfiabilityToNext != no_reduction_type_and_operator);
				updateOperation._makeReductionSatisfied = updatedStatus._propagatesReductionSatisfiabilityToNext;
			}
			
			// Make Next Topmost
			if (initialStatus._makesNextTopmost != updatedStatus._makesNextTopmost) {
				assert(!initialStatus._makesNextTopmost);
				updateOperation._makeTopmost = true;
			}
			
			if (initialStatus._propagatesTopLevel != updatedStatus._propagatesTopLevel) {
				assert(!initialStatus._propagatesTopLevel);
				updateOperation._makeTopLevel = true;
			}
			
			if (!updateOperation.empty()) {
				hpDependencyData._delayedOperations.emplace_back(updateOperation);
			}
		}
		
		// Propagation to Fragments
		if (access->hasSubaccesses()) {
			UpdateOperation updateOperation(DataAccessLink(task, fragment_type), access->getAccessRegion());
			
			if (initialStatus._propagatesReadSatisfiabilityToFragments != updatedStatus._propagatesReadSatisfiabilityToFragments) {
				assert(!initialStatus._propagatesReadSatisfiabilityToFragments);
				updateOperation._makeReadSatisfied = true;
			}
			
			if (initialStatus._propagatesWriteSatisfiabilityToFragments != updatedStatus._propagatesWriteSatisfiabilityToFragments) {
				assert(!initialStatus._propagatesWriteSatisfiabilityToFragments);
				updateOperation._makeWriteSatisfied = true;
			}
			
			if (initialStatus._propagatesConcurrentSatisfiabilityToFragments != updatedStatus._propagatesConcurrentSatisfiabilityToFragments) {
				assert(!initialStatus._propagatesConcurrentSatisfiabilityToFragments);
				updateOperation._makeConcurrentSatisfied = true;
			}
			
			if (initialStatus._propagatesReductionSatisfiabilityToFragments != updatedStatus._propagatesReductionSatisfiabilityToFragments) {
				assert(updatedStatus._propagatesReductionSatisfiabilityToFragments != no_reduction_type_and_operator);
				updateOperation._makeReductionSatisfied = updatedStatus._propagatesReductionSatisfiabilityToFragments;
			}
			
			if (!updateOperation.empty()) {
				hpDependencyData._delayedOperations.emplace_back(updateOperation);
			}
		}
		
		// Bottom Map Updates
		if (access->hasSubaccesses()) {
			if (
				initialStatus._linksBottomMapAccessesToNextAndInhibitsPropagation
				!= updatedStatus._linksBottomMapAccessesToNextAndInhibitsPropagation
			) {
				BottomMapUpdateOperation bottomMapUpdateOperation(access->getAccessRegion());
				
				bottomMapUpdateOperation._linkBottomMapAccessesToNext = true;
				bottomMapUpdateOperation._next = access->getNext();
				
				bottomMapUpdateOperation._inhibitReadPropagation = (access->getType() == READ_ACCESS_TYPE);
				assert(!updatedStatus._propagatesWriteSatisfiabilityToNext);
				bottomMapUpdateOperation._inhibitConcurrentPropagation = (access->getType() == CONCURRENT_ACCESS_TYPE);
				bottomMapUpdateOperation._inhibitReductionPropagation =
					(access->getType() == REDUCTION_ACCESS_TYPE ? access->getReductionTypeAndOperatorIndex() : no_reduction_type_and_operator);
				
				processBottomMapUpdate(bottomMapUpdateOperation, accessStructures, task, hpDependencyData);
			}
		}
		
		// Removable
		if (initialStatus._isRemovable != updatedStatus._isRemovable) {
			assert(!initialStatus._isRemovable);
			
			assert(accessStructures._removalBlockers > 0);
			accessStructures._removalBlockers--;
			access->markAsDiscounted();
			
			// The last taskwait fragment that finishes removes the blocking over the task
			if (access->getObjectType() == taskwait_type) {
				assert(accessStructures._liveTaskwaitFragmentCount > 0);
				accessStructures._liveTaskwaitFragmentCount--;
				
				if (accessStructures._liveTaskwaitFragmentCount == 0) {
					if (task->decreaseRemovalBlockingCount()) {
						hpDependencyData._removableTasks.push_back(task);
					}
				}
			}
			
			if (access->hasNext()) {
				Instrument::unlinkedDataAccesses(
					access->getInstrumentationId(),
					access->getNext()._task->getInstrumentationTaskId(),
					(Instrument::access_object_type_t) access->getNext()._objectType,
					/* direct */ true
				);
			} else {
				assert((access->getObjectType() == taskwait_type) || (access->getObjectType() == top_level_sink_type));
				removeBottomMapTaskwaitOrTopLevelSink(access, accessStructures, task);
			}
			
			if (accessStructures._removalBlockers == 0) {
				if (task->decreaseRemovalBlockingCount()) {
					hpDependencyData._removableTasks.push_back(task);
				}
			}
		}
	}
	
	
	static inline void removeBottomMapTaskwaitOrTopLevelSink(
		DataAccess *access, TaskDataAccesses &accessStructures, __attribute__((unused)) Task *task 
	) {
		assert(access != nullptr);
		assert(task != nullptr);
		assert(access->getOriginator() == task);
		assert(accessStructures._lock.isLockedByThisThread());
		assert((access->getObjectType() == taskwait_type) || (access->getObjectType() == top_level_sink_type));
		
		accessStructures._subaccessBottomMap.processIntersecting(
			access->getAccessRegion(),
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);
				assert(access->getAccessRegion().fullyContainedIn(bottomMapEntry->getAccessRegion()));
				assert(bottomMapEntry->_link._task == task);
				assert(bottomMapEntry->_link._objectType == access->getObjectType());
				
				if (access->getAccessRegion() == bottomMapEntry->getAccessRegion()) {
					accessStructures._subaccessBottomMap.erase(bottomMapEntry);
					delete bottomMapEntry;
				} else {
					fragmentBottomMapEntry(
						bottomMapEntry, access->getAccessRegion(),
						accessStructures,
						/* remove intersection */ true
					);
				}
				
				return true;
			}
		);
		
		accessStructures._taskwaitFragments.erase(access);
		delete access;
	}
	
	
	static inline DataAccess *createAccess(
		Task *originator,
		DataAccessObjectType objectType,
		DataAccessType accessType, bool weak, DataAccessRegion region,
		reduction_type_and_operator_index_t reductionTypeAndOperatorIndex,
		DataAccess::status_t status = 0, DataAccessLink next = DataAccessLink()
	) {
		// Regular object duplication
		DataAccess *dataAccess = new DataAccess(
			objectType,
			accessType, weak, originator, region,
			reductionTypeAndOperatorIndex,
			Instrument::data_access_id_t(),
			status, next
		);
		
		return dataAccess;
	}
	
	
	static inline void upgradeAccess(
		DataAccess *dataAccess, DataAccessType accessType, bool weak, reduction_type_and_operator_index_t reductionTypeAndOperatorIndex
	) {
		assert(dataAccess != nullptr);
		assert(!dataAccess->hasBeenDiscounted());
		
		bool newWeak = dataAccess->isWeak() && weak;
		
		DataAccessType newDataAccessType = accessType;
		if (accessType != dataAccess->getType()) {
			FatalErrorHandler::failIf(
				(accessType == REDUCTION_ACCESS_TYPE) || (dataAccess->getType() == REDUCTION_ACCESS_TYPE),
				"Task ",
				(dataAccess->getOriginator()->getTaskInfo()->implementations[0].task_label != nullptr ?
					dataAccess->getOriginator()->getTaskInfo()->implementations[0].task_label :
					dataAccess->getOriginator()->getTaskInfo()->implementations[0].declaration_source
				),
				" has non-reduction accesses that overlap a reduction"
			);
			newDataAccessType = READWRITE_ACCESS_TYPE;
		} else {
			FatalErrorHandler::failIf(
				(accessType == REDUCTION_ACCESS_TYPE)
					&& (dataAccess->getReductionTypeAndOperatorIndex() != reductionTypeAndOperatorIndex),
				"Task ",
				(dataAccess->getOriginator()->getTaskInfo()->implementations[0].task_label != nullptr ?
					dataAccess->getOriginator()->getTaskInfo()->implementations[0].task_label :
					dataAccess->getOriginator()->getTaskInfo()->implementations[0].declaration_source
				),
				" has two overlapping reductions over different types or with different operators"
			);
		}
		
		dataAccess->upgrade(newWeak, newDataAccessType);
	}
	
	
	// NOTE: locking should be handled from the outside
	static inline DataAccess *duplicateDataAccess(
		DataAccess const &toBeDuplicated,
		__attribute__((unused)) TaskDataAccesses &accessStructures
	) {
		assert(toBeDuplicated.getOriginator() != nullptr);
		assert(!accessStructures.hasBeenDeleted());
		assert(!toBeDuplicated.hasBeenDiscounted());
		
		// Regular object duplication
		DataAccess *newFragment = createAccess(
			toBeDuplicated.getOriginator(),
			toBeDuplicated.getObjectType(),
			toBeDuplicated.getType(), toBeDuplicated.isWeak(), toBeDuplicated.getAccessRegion(),
			toBeDuplicated.getReductionTypeAndOperatorIndex(),
			toBeDuplicated.getStatus(), toBeDuplicated.getNext()
		);

		// Copy symbols
		for (int i = 0; i < toBeDuplicated.getOriginator()->getSymbolNum(); ++i){
			if(toBeDuplicated.isSymbol(i)){
				newFragment->setSymbol(i);
			}
		}		

		newFragment->clearRegistered();
		
		return newFragment;
	}
	
	
#ifndef NDEBUG
	static bool noAccessIsReachable(TaskDataAccesses &accessStructures)
	{
		assert(!accessStructures.hasBeenDeleted());
		return accessStructures._accesses.processAll(
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				return !position->isReachable();
			}
		);
	}
#endif
	
	
	static inline BottomMapEntry *fragmentBottomMapEntry(
		BottomMapEntry *bottomMapEntry, DataAccessRegion region,
		TaskDataAccesses &accessStructures, bool removeIntersection
	) {
		if (bottomMapEntry->getAccessRegion().fullyContainedIn(region)) {
			// Nothing to fragment
			return bottomMapEntry;
		}
		
		assert(!accessStructures.hasBeenDeleted());
		assert(accessStructures._lock.isLockedByThisThread());
		
		TaskDataAccesses::subaccess_bottom_map_t::iterator position =
			accessStructures._subaccessBottomMap.iterator_to(*bottomMapEntry);
		position = accessStructures._subaccessBottomMap.fragmentByIntersection(
			position, region,
			removeIntersection,
			[&](BottomMapEntry const &toBeDuplicated) -> BottomMapEntry * {
				return new BottomMapEntry(DataAccessRegion(), toBeDuplicated._link, toBeDuplicated._accessType);
			},
			[&](__attribute__((unused)) BottomMapEntry *fragment, __attribute__((unused)) BottomMapEntry *originalBottomMapEntry) {
			}
		);
		
		if (!removeIntersection) {
			bottomMapEntry = &(*position);
			assert(bottomMapEntry != nullptr);
			assert(bottomMapEntry->getAccessRegion().fullyContainedIn(region));
			
			return bottomMapEntry;
		} else {
			return nullptr;
		}
	}
	
	
	static inline void setUpNewFragment(
		DataAccess *fragment, DataAccess *originalDataAccess,
		TaskDataAccesses &accessStructures
	) {
		if (fragment != originalDataAccess) {
			CPUDependencyData hpDependencyData;
			
			DataAccessStatusEffects initialStatus(fragment);
			fragment->setUpNewFragment(originalDataAccess->getInstrumentationId());
			fragment->setRegistered();
			DataAccessStatusEffects updatedStatus(fragment);
			
			handleDataAccessStatusChanges(
				initialStatus, updatedStatus,
				fragment, accessStructures, fragment->getOriginator(),
				hpDependencyData
			);
			
			assert(hpDependencyData.empty());
		}
	}
	
	
	static inline DataAccess *fragmentAccessObject(
		DataAccess *dataAccess, DataAccessRegion const &region,
		TaskDataAccesses &accessStructures
	) {
		assert(!dataAccess->hasBeenDiscounted());
		assert(dataAccess->getObjectType() == access_type);
		
		if (dataAccess->getAccessRegion().fullyContainedIn(region)) {
			// Nothing to fragment
			return dataAccess;
		}
		
		TaskDataAccesses::accesses_t::iterator position =
			accessStructures._accesses.iterator_to(*dataAccess);
		position = accessStructures._accesses.fragmentByIntersection(
			position, region,
			false,
			[&](DataAccess const &toBeDuplicated) -> DataAccess * {
				assert(toBeDuplicated.isRegistered());
				return duplicateDataAccess(toBeDuplicated, accessStructures);
			},
			[&](DataAccess *fragment, DataAccess *originalDataAccess) {
				setUpNewFragment(fragment, originalDataAccess, accessStructures);
			}
		);
		
		dataAccess = &(*position);
		assert(dataAccess != nullptr);
		assert(dataAccess->getAccessRegion().fullyContainedIn(region));
		
		return dataAccess;
	}
	
	
	static inline DataAccess *fragmentFragmentObject(
		DataAccess *dataAccess, DataAccessRegion const &region,
		TaskDataAccesses &accessStructures
	) {
		assert(!dataAccess->hasBeenDiscounted());
		assert(dataAccess->getObjectType() == fragment_type);
		
		if (dataAccess->getAccessRegion().fullyContainedIn(region)) {
			// Nothing to fragment
			return dataAccess;
		}
		
		TaskDataAccesses::access_fragments_t::iterator position =
			accessStructures._accessFragments.iterator_to(*dataAccess);
		position = accessStructures._accessFragments.fragmentByIntersection(
			position, region,
			false,
			[&](DataAccess const &toBeDuplicated) -> DataAccess * {
				assert(toBeDuplicated.isRegistered());
				return duplicateDataAccess(toBeDuplicated, accessStructures);
			},
			[&](DataAccess *fragment, DataAccess *originalDataAccess) {
				setUpNewFragment(fragment, originalDataAccess, accessStructures);
			}
		);
		
		dataAccess = &(*position);
		assert(dataAccess != nullptr);
		assert(dataAccess->getAccessRegion().fullyContainedIn(region));
		
		return dataAccess;
	}
	
	
	static inline DataAccess *fragmentTaskwaitFragmentObject(
		DataAccess *dataAccess, DataAccessRegion const &region,
		TaskDataAccesses &accessStructures
	) {
		assert(!dataAccess->hasBeenDiscounted());
		assert((dataAccess->getObjectType() == taskwait_type) || (dataAccess->getObjectType() == top_level_sink_type));
		
		if (dataAccess->getAccessRegion().fullyContainedIn(region)) {
			// Nothing to fragment
			return dataAccess;
		}
		
		TaskDataAccesses::taskwait_fragments_t::iterator position =
			accessStructures._taskwaitFragments.iterator_to(*dataAccess);
		position = accessStructures._taskwaitFragments.fragmentByIntersection(
			position, region,
			false,
			[&](DataAccess const &toBeDuplicated) -> DataAccess * {
				assert(toBeDuplicated.isRegistered());
				return duplicateDataAccess(toBeDuplicated, accessStructures);
			},
			[&](DataAccess *fragment, DataAccess *originalDataAccess) {
				setUpNewFragment(fragment, originalDataAccess, accessStructures);
			}
		);
		
		dataAccess = &(*position);
		assert(dataAccess != nullptr);
		assert(dataAccess->getAccessRegion().fullyContainedIn(region));
		
		return dataAccess;
	}
	
	
	static inline DataAccess *fragmentAccess(
		DataAccess *dataAccess, DataAccessRegion const &region,
		TaskDataAccesses &accessStructures
	) {
		assert(dataAccess != nullptr);
		// assert(accessStructures._lock.isLockedByThisThread()); // Not necessary when fragmenting an access that is not reachable
		assert(accessStructures._lock.isLockedByThisThread() || noAccessIsReachable(accessStructures));
		assert(&dataAccess->getOriginator()->getDataAccesses() == &accessStructures);
		assert(!accessStructures.hasBeenDeleted());
		assert(!dataAccess->hasBeenDiscounted());
		
		if (dataAccess->getAccessRegion().fullyContainedIn(region)) {
			// Nothing to fragment
			return dataAccess;
		}
		
		// Partial overlapping of reductions is not supported at this time
		assert(dataAccess->getType() != REDUCTION_ACCESS_TYPE);
		
		if (dataAccess->getObjectType() == access_type) {
			return fragmentAccessObject(dataAccess, region, accessStructures);
		} else if (dataAccess->getObjectType() == fragment_type) {
			return fragmentFragmentObject(dataAccess, region, accessStructures);
		} else {
			assert((dataAccess->getObjectType() == taskwait_type) || (dataAccess->getObjectType() == top_level_sink_type));
			return fragmentTaskwaitFragmentObject(dataAccess, region, accessStructures);
		}
	}
	
	
	//! Process all the originators that have become ready
	static inline void processSatisfiedOriginators(
		/* INOUT */ CPUDependencyData &hpDependencyData,
		ComputePlace *computePlace,
		bool fromBusyThread
	) {
		// NOTE: This is done without the lock held and may be slow since it can enter the scheduler
		for (Task *satisfiedOriginator : hpDependencyData._satisfiedOriginators) {
			assert(satisfiedOriginator != 0);
			
			ComputePlace *idleComputePlace = Scheduler::addReadyTask(
				satisfiedOriginator, computePlace,
				(fromBusyThread ?
					SchedulerInterface::SchedulerInterface::BUSY_COMPUTE_PLACE_TASK_HINT
					: SchedulerInterface::SchedulerInterface::SIBLING_TASK_HINT
				)
			);
			
			if (idleComputePlace != nullptr) {
				ThreadManager::resumeIdle((CPU *) idleComputePlace);
			}
		}
		
		hpDependencyData._satisfiedOriginators.clear();
	}
	
	
	static void applyUpdateOperationOnAccess(
		UpdateOperation const &updateOperation,
		DataAccess *access, TaskDataAccesses &accessStructures,
		/* OUT */ CPUDependencyData &hpDependencyData
	) {
		// Fragment if necessary
		access = fragmentAccess(access, updateOperation._region, accessStructures);
		assert(access != nullptr);
		
		DataAccessStatusEffects initialStatus(access);
		
		// Read, Write, Concurrent Satisfiability
		if (updateOperation._makeReadSatisfied) {
			access->setReadSatisfied();
		}
		if (updateOperation._makeWriteSatisfied) {
			access->setWriteSatisfied();
		}
		if (updateOperation._makeConcurrentSatisfied) {
			access->setConcurrentSatisfied();
		}
		
		// Reduction Satisfiability
		if (updateOperation._makeReductionSatisfied == any_reduction_type_and_operator) {
			access->setAnyReductionSatisfied();
		} else if (
			(updateOperation._makeReductionSatisfied != no_reduction_type_and_operator)
			&& (updateOperation._makeReductionSatisfied == access->getReductionTypeAndOperatorIndex())
		) {
			access->setMatchingReductionSatisfied();
		}
		
		// Topmost
		if (updateOperation._makeTopmost) {
			access->setTopmost();
		}
		
		// Top Level
		if (updateOperation._makeTopLevel) {
			access->setTopLevel();
		}
		
		DataAccessStatusEffects updatedStatus(access);
		
		handleDataAccessStatusChanges(
			initialStatus, updatedStatus,
			access, accessStructures, updateOperation._target._task, 
			hpDependencyData
		);
	}
	
	static void processUpdateOperation(
		UpdateOperation const &updateOperation,
		/* OUT */ CPUDependencyData &hpDependencyData
	) {
		assert(!updateOperation.empty());
		TaskDataAccesses &accessStructures = updateOperation._target._task->getDataAccesses();
		
		if (updateOperation._target._objectType == access_type) {
			// Update over Accesses
			accessStructures._accesses.processIntersecting(
				updateOperation._region,
				[&] (TaskDataAccesses::accesses_t::iterator accessPosition) -> bool {
					DataAccess *access = &(*accessPosition);
					
					applyUpdateOperationOnAccess(updateOperation, access, accessStructures, hpDependencyData);
					
					return true;
				}
			);
		} else if (updateOperation._target._objectType == fragment_type) {
			// Update over Fragments
			accessStructures._accessFragments.processIntersecting(
				updateOperation._region,
				[&] (TaskDataAccesses::access_fragments_t::iterator fragmentPosition) -> bool {
					DataAccess *fragment = &(*fragmentPosition);
					
					applyUpdateOperationOnAccess(updateOperation, fragment, accessStructures, hpDependencyData);
					
					return true;
				}
			);
		} else {
			// Update over Taskwait Fragments
			assert((updateOperation._target._objectType == taskwait_type) || (updateOperation._target._objectType == top_level_sink_type));
			accessStructures._taskwaitFragments.processIntersecting(
				updateOperation._region,
				[&] (TaskDataAccesses::access_fragments_t::iterator position) -> bool {
					DataAccess *taskwaitFragment = &(*position);
					
					applyUpdateOperationOnAccess(updateOperation, taskwaitFragment, accessStructures, hpDependencyData);
					
					return true;
				}
			);
		}
	}
	
	
	static inline void processDelayedOperations(
		/* INOUT */ CPUDependencyData &hpDependencyData
	) {
		Task *lastLocked = nullptr;
		
		while (!hpDependencyData._delayedOperations.empty()) {
			UpdateOperation &delayedOperation = hpDependencyData._delayedOperations.front();
			
			assert(delayedOperation._target._task != nullptr);
			if (delayedOperation._target._task != lastLocked) {
				if (lastLocked != nullptr) {
					lastLocked->getDataAccesses()._lock.unlock();
				}
				lastLocked = delayedOperation._target._task;
				lastLocked->getDataAccesses()._lock.lock();
			}
			
			processUpdateOperation(delayedOperation, hpDependencyData);
			
			hpDependencyData._delayedOperations.pop_front();
		}
		
		if (lastLocked != nullptr) {
			lastLocked->getDataAccesses()._lock.unlock();
		}
	}
	
	
	static void processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
		CPUDependencyData &hpDependencyData,
		ComputePlace *computePlace,
		bool fromBusyThread
	) {
		assert(computePlace != nullptr);
		
#if NO_DEPENDENCY_DELAYED_OPERATIONS
#else
		processDelayedOperations(hpDependencyData);
#endif
		
		processSatisfiedOriginators(hpDependencyData, computePlace, fromBusyThread);
		assert(hpDependencyData._satisfiedOriginators.empty());
		
		handleRemovableTasks(hpDependencyData._removableTasks, computePlace);
	}
	
	
	// NOTE: This method does not create the bottom map entry
	static inline DataAccess *createInitialFragment(
		TaskDataAccesses::accesses_t::iterator accessPosition,
		TaskDataAccesses &accessStructures,
		DataAccessRegion subregion
	) {
		DataAccess *dataAccess = &(*accessPosition);
		assert(dataAccess != nullptr);
		assert(!accessStructures.hasBeenDeleted());
		
		assert(!accessStructures._accessFragments.contains(dataAccess->getAccessRegion()));
		
		Instrument::data_access_id_t instrumentationId =
			Instrument::createdDataSubaccessFragment(dataAccess->getInstrumentationId());
		DataAccess *fragment = new DataAccess(
			fragment_type,
			dataAccess->getType(),
			dataAccess->isWeak(),
			dataAccess->getOriginator(),
			dataAccess->getAccessRegion(),
			dataAccess->getReductionTypeAndOperatorIndex(),
			instrumentationId
		);

		// TODO what is this? How do I set symbols?
		
		fragment->inheritFragmentStatus(dataAccess);
#ifndef NDEBUG
		fragment->setReachable();
#endif
		
		assert(fragment->readSatisfied() || !fragment->writeSatisfied());
		
		accessStructures._accessFragments.insert(*fragment);
		fragment->setInBottomMap();
		
		// NOTE: This may in the future need to be included in the common status changes code
		dataAccess->setHasSubaccesses();
		
		if (subregion != dataAccess->getAccessRegion()) {
			dataAccess->getAccessRegion().processIntersectingFragments(
				subregion,
				[&](DataAccessRegion excludedSubregion) {
					 BottomMapEntry *bottomMapEntry = new BottomMapEntry(
						excludedSubregion,
						DataAccessLink(dataAccess->getOriginator(), fragment_type),
						dataAccess->getType()
					);
					accessStructures._subaccessBottomMap.insert(*bottomMapEntry);
				},
				[&](__attribute__((unused)) DataAccessRegion intersection) {
				},
				[&](__attribute__((unused)) DataAccessRegion unmatchedRegion) {
					// This part is not covered by the access
				}
			);
		}
		
		return fragment;
	}
	
	
	template <typename ProcessorType>
	static inline bool followLink(
		DataAccessLink const &link,
		DataAccessRegion const &region,
		ProcessorType processor
	) {
		Task *task = link._task;
		assert(task != nullptr);
		
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(accessStructures._lock.isLockedByThisThread());
		
		if (link._objectType == access_type) {
			return accessStructures._accesses.processIntersecting(
				region,
				[&] (TaskDataAccesses::accesses_t::iterator position) -> bool {
					DataAccess *access = &(*position);
					assert(!access->hasBeenDiscounted());
					
					access = fragmentAccessObject(access, region, accessStructures);
					
					return processor(access);
				}
			);
		} else if (link._objectType == fragment_type) {
			return accessStructures._accessFragments.processIntersecting(
				region,
				[&] (TaskDataAccesses::access_fragments_t::iterator position) -> bool {
					DataAccess *access = &(*position);
					assert(!access->hasBeenDiscounted());
					
					access = fragmentFragmentObject(access, region, accessStructures);
					
					return processor(access);
				}
			);
		} else {
			assert((link._objectType == taskwait_type) || (link._objectType == top_level_sink_type));
			return accessStructures._taskwaitFragments.processIntersecting(
				region,
				[&] (TaskDataAccesses::taskwait_fragments_t::iterator position) -> bool {
					DataAccess *access = &(*position);
					assert(!access->hasBeenDiscounted());
					
					access = fragmentTaskwaitFragmentObject(access, region, accessStructures);
					
					return processor(access);
				}
			);
		}
	}
	
	
	template <typename MatchingProcessorType, typename MissingProcessorType>
	static inline bool foreachBottomMapMatchPossiblyCreatingInitialFragmentsAndMissingRegion(
		Task *parent, TaskDataAccesses &parentAccessStructures,
		DataAccessRegion region,
		MatchingProcessorType matchingProcessor, MissingProcessorType missingProcessor
	) {
		assert(parent != nullptr);
		assert((&parentAccessStructures) == (&parent->getDataAccesses()));
		assert(!parentAccessStructures.hasBeenDeleted());
		
		return parentAccessStructures._subaccessBottomMap.processIntersectingAndMissing(
			region,
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);
				
				DataAccessRegion subregion = region.intersect(bottomMapEntry->getAccessRegion());
				BottomMapEntryContents bmeContents = *bottomMapEntry;
				
				DataAccessLink target = bmeContents._link;
				assert(target._task != nullptr);
				
				bool result = true;
				if (target._task != parent) {
					TaskDataAccesses &subtaskAccessStructures = target._task->getDataAccesses();
					
					subtaskAccessStructures._lock.lock();
					
					// For each access of the subtask that matches
					result = followLink(
						target, subregion,
						[&](DataAccess *previous) -> bool {
							assert(!previous->hasNext());
							assert(previous->isInBottomMap());
							
							return matchingProcessor(previous, bmeContents);
						}
					);
					
					subtaskAccessStructures._lock.unlock();
				} else {
					// A fragment
					assert(target._objectType == fragment_type);
					
					// For each fragment of the parent that matches
					result = followLink(
						target, subregion,
						[&](DataAccess *previous) -> bool {
							assert(!previous->hasNext());
							assert(previous->isInBottomMap());
							
							return matchingProcessor(previous, bmeContents);
						}
					);
				}
				
				bottomMapEntry = fragmentBottomMapEntry(bottomMapEntry, subregion, parentAccessStructures);
				parentAccessStructures._subaccessBottomMap.erase(*bottomMapEntry);
				delete bottomMapEntry;
				
				return result;
			},
			[&](DataAccessRegion missingRegion) -> bool {
				parentAccessStructures._accesses.processIntersectingAndMissing(
					missingRegion,
					[&](TaskDataAccesses::accesses_t::iterator superaccessPosition) -> bool {
						DataAccessStatusEffects initialStatus;
						
						DataAccess *previous = createInitialFragment(
							superaccessPosition, parentAccessStructures,
							missingRegion
						);
						assert(previous != nullptr);
						assert(previous->getObjectType() == fragment_type);
						
						previous->setTopmost();
						previous->setRegistered();
						
						DataAccessStatusEffects updatedStatus(previous);
						
						BottomMapEntryContents bmeContents(
							DataAccessLink(parent, fragment_type),
							previous->getType()
						);
						
						{
							CPUDependencyData hpDependencyData;
							handleDataAccessStatusChanges(
								initialStatus, updatedStatus,
								previous, parentAccessStructures, parent,
								hpDependencyData
							);
							assert(hpDependencyData.empty());
						}
						
						previous = fragmentAccess(previous, missingRegion, parentAccessStructures);
						
						return matchingProcessor(previous, bmeContents);
					},
					[&](DataAccessRegion regionUncoveredByParent) -> bool {
						return missingProcessor(regionUncoveredByParent);
					}
				);
				
				return true;
			}
		);
	}
	
	
	template <typename ProcessorType, typename BottomMapEntryProcessorType>
	static inline void foreachBottomMapMatch(
		DataAccessRegion const &region,
		TaskDataAccesses &accessStructures, Task *task,
		ProcessorType processor,
		BottomMapEntryProcessorType bottomMapEntryProcessor = [] (BottomMapEntry *) {}
	) {
		assert(!accessStructures.hasBeenDeleted());
		assert(accessStructures._lock.isLockedByThisThread());
		
		accessStructures._subaccessBottomMap.processIntersecting(
			region,
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);
				
				DataAccessLink target = bottomMapEntry->_link;
				assert(target._task != nullptr);
				
				DataAccessRegion subregion = region.intersect(bottomMapEntry->getAccessRegion());
				
				if (target._task != task) {
					// An access from a subtask
					
					TaskDataAccesses &subtaskAccessStructures = target._task->getDataAccesses();
					subtaskAccessStructures._lock.lock();
					
					// For each access of the subtask that matches
					followLink(
						target, subregion,
						[&](DataAccess *subaccess) -> bool {
							assert(subaccess->isReachable());
							assert(subaccess->isInBottomMap());
							
							processor(subaccess, subtaskAccessStructures, target._task);
							
							return true;
						}
					);
					
					subtaskAccessStructures._lock.unlock();
				} else {
					// A fragment from the current task, a taskwait fragment, or a top level sink
					assert(
						(target._objectType == fragment_type)
						|| (target._objectType == taskwait_type)
						|| (target._objectType == top_level_sink_type)
					);
					
					followLink(
						target, subregion,
						[&](DataAccess *fragment) -> bool {
							assert(fragment->isReachable());
							assert(fragment->isInBottomMap());
							
							processor(fragment, accessStructures, task);
							
							return true;
						}
					);
				}
				
				bottomMapEntryProcessor(bottomMapEntry);
				
				return true;
			}
		);
	}
	
	
	template <typename ProcessorType, typename BottomMapEntryProcessorType>
	static inline void foreachBottomMapEntry(
		TaskDataAccesses &accessStructures, Task *task,
		ProcessorType processor,
		BottomMapEntryProcessorType bottomMapEntryProcessor = [] (BottomMapEntry *) {}
	) {
		assert(!accessStructures.hasBeenDeleted());
		assert(accessStructures._lock.isLockedByThisThread());
		
		accessStructures._subaccessBottomMap.processAll(
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);
				
				DataAccessLink target = bottomMapEntry->_link;
				assert(target._task != nullptr);
				
				DataAccessRegion const &subregion = bottomMapEntry->getAccessRegion();
				
				if (target._task != task) {
					// An access from a subtask
					
					TaskDataAccesses &subtaskAccessStructures = target._task->getDataAccesses();
					subtaskAccessStructures._lock.lock();
					
					// For each access of the subtask that matches
					followLink(
						target, subregion,
						[&](DataAccess *subaccess) -> bool {
							assert(subaccess->isReachable());
							assert(subaccess->isInBottomMap());
							
							processor(subaccess, subtaskAccessStructures, target._task);
							
							return true;
						}
					);
					
					subtaskAccessStructures._lock.unlock();
				} else {
					// A fragment from the current task
					assert(target._objectType == fragment_type);
					
					followLink(
						target, subregion,
						[&](DataAccess *fragment) -> bool {
							assert(fragment->isReachable());
							assert(fragment->isInBottomMap());
							
							processor(fragment, accessStructures, task);
							
							return true;
						}
					);
				}
				
				bottomMapEntryProcessor(bottomMapEntry);
				
				return true;
			}
		);
	}
	
	
	static inline void processBottomMapUpdate(
		BottomMapUpdateOperation &operation,
		TaskDataAccesses &accessStructures, Task *task,
		/* OUT */ CPUDependencyData &hpDependencyData
	) {
		assert(task != nullptr);
		assert(!operation.empty());
		assert(!operation._region.empty());
		assert(!accessStructures.hasBeenDeleted());
		assert(accessStructures._lock.isLockedByThisThread());
		
		assert(operation._linkBottomMapAccessesToNext);
		foreachBottomMapMatch(
			operation._region,
			accessStructures, task,
			[&] (DataAccess *access, TaskDataAccesses &currentAccessStructures, Task *currentTask) {
				DataAccessStatusEffects initialStatus(access);
				
				if (operation._inhibitReadPropagation) {
					assert(access->canPropagateReadSatisfiability());
					access->unsetCanPropagateReadSatisfiability();
				}
				
				if (operation._inhibitConcurrentPropagation) {
					assert(access->canPropagateConcurrentSatisfiability());
					access->unsetCanPropagateConcurrentSatisfiability();
				}
				
				if (operation._inhibitReductionPropagation == any_reduction_type_and_operator) {
					access->unsetCanPropagateAnyReductionSatisfiability();
				} else if (
					(operation._inhibitReductionPropagation != no_reduction_type_and_operator)
					&& (access->getReductionTypeAndOperatorIndex() == operation._inhibitReductionPropagation)
				) {
					access->unsetCanPropagateMatchingReductionSatisfiability();
				}
				
				assert(!access->hasNext());
				access->setNext(operation._next);
				
				DataAccessStatusEffects updatedStatus(access);
				
				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					access, currentAccessStructures, currentTask,
					hpDependencyData
				);
			},
			[] (BottomMapEntry *) {}
		);
	}
	
	
	static inline void replaceMatchingInBottomMapLinkAndPropagate(
		DataAccessLink const &next, TaskDataAccesses &accessStructures,
		DataAccessRegion region,
		Task *parent, TaskDataAccesses &parentAccessStructures,
		/* inout */ CPUDependencyData &hpDependencyData
	) {
		assert(parent != nullptr);
		assert(next._task != nullptr);
		assert(!accessStructures.hasBeenDeleted());
		assert(!parentAccessStructures.hasBeenDeleted());
		
		bool local = false;
		#ifndef NDEBUG
			bool lastWasLocal = false;
			bool first = true;
		#endif
		
		DataAccessType parentAccessType = NO_ACCESS_TYPE;
		
		// Link accesses to their corresponding predecessor and removes the bottom map entry
		foreachBottomMapMatchPossiblyCreatingInitialFragmentsAndMissingRegion(
			parent, parentAccessStructures,
			region,
			[&](DataAccess *previous, BottomMapEntryContents const &bottomMapEntryContents) -> bool {
				assert(previous != nullptr);
				assert(previous->isReachable());
				assert(!previous->hasBeenDiscounted());
				assert(!previous->hasNext());
				
				Task *previousTask = previous->getOriginator();
				assert(previousTask != nullptr);
				
				parentAccessType = bottomMapEntryContents._accessType;
				local = (bottomMapEntryContents._accessType == NO_ACCESS_TYPE);
				
				#ifndef NDEBUG
					if (!first) {
						assert((local == lastWasLocal) && "This fails with wrongly nested regions");
					}
					first = false;
					lastWasLocal = local;
				#endif
				
				TaskDataAccesses &previousAccessStructures = previousTask->getDataAccesses();
				assert(!previousAccessStructures.hasBeenDeleted());
				assert(previous->getAccessRegion().fullyContainedIn(region));
				
				DataAccessStatusEffects initialStatus(previous);
				
				// Link the dataAccess
				previous->setNext(next);
				previous->unsetInBottomMap();
				
				DataAccessStatusEffects updatedStatus(previous);
				
				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					previous, previousAccessStructures, previousTask,
					hpDependencyData
				);
				
				return true;
			},
			[&](DataAccessRegion missingRegion) -> bool {
				assert(!parentAccessStructures._accesses.contains(missingRegion));
				
				// Not part of the parent
				local = true;
				
				#ifndef NDEBUG
					if (!first) {
						assert((local == lastWasLocal) && "This fails with wrongly nested regions");
					}
					first = false;
					lastWasLocal = local;
				#endif
				
				// Holes in the parent bottom map that are not in the parent accesses become fully satisfied
				std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock); // Need the lock since an access of data allocated in the parent may partially overlap a previous one
				accessStructures._accesses.processIntersecting(
					missingRegion,
					[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
						DataAccess *targetAccess = &(*position);
						assert(targetAccess != nullptr);
						assert(!targetAccess->hasBeenDiscounted());
						
						targetAccess = fragmentAccess(targetAccess, missingRegion, accessStructures);
						
						DataAccessStatusEffects initialStatus(targetAccess);
						targetAccess->setReadSatisfied();
						targetAccess->setWriteSatisfied();
						targetAccess->setConcurrentSatisfied();
						targetAccess->setAnyReductionSatisfied();
						targetAccess->setMatchingReductionSatisfied();
						targetAccess->setTopmost();
						targetAccess->setTopLevel();
						DataAccessStatusEffects updatedStatus(targetAccess);
						
						// TODO: We could mark in the task that there are local accesses (and remove the mark in taskwaits)
						
						handleDataAccessStatusChanges(
							initialStatus, updatedStatus,
							targetAccess, accessStructures, next._task,
							hpDependencyData
						);
						
						return true;
					}
				);
				
				return true;
			}
		);
		
		// Add the entry to the bottom map
		BottomMapEntry *bottomMapEntry = new BottomMapEntry(region, next, parentAccessType);
		parentAccessStructures._subaccessBottomMap.insert(*bottomMapEntry);
	}
	
	
	static inline void linkTaskAccesses(
		/* OUT */ CPUDependencyData &hpDependencyData,
		Task *task
	) {
		assert(task != nullptr);
		
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());
		
		if (accessStructures._accesses.empty()) {
			return;
		}
		
		Task *parent = task->getParent();
		assert(parent != nullptr);
		
		TaskDataAccesses &parentAccessStructures = parent->getDataAccesses();
		assert(!parentAccessStructures.hasBeenDeleted());
		
		
		std::lock_guard<TaskDataAccesses::spinlock_t> parentGuard(parentAccessStructures._lock);
		std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);
		
		// Create any initial missing fragments in the parent, link the previous accesses
		// and possibly some parent fragments to the new task, and create propagation
		// operations from the previous accesses to the new task.
		// 
		// The new task cannot be locked since it may have a predecessor multiple times,
		// and that could produce a dead lock if the latter is finishing (this one would
		// lock the new task first, and the predecessor later; the finishing task would
		// do the same in the reverse order). However, we need to protect the traversal
		// itself, since an already linked predecessor may produce fragmentation and thus
		// may rebalance the tree. Therefore, we just lock for advancing the iteration.
		accessStructures._accesses.processAll(
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *dataAccess = &(*position);
				assert(dataAccess != nullptr);
				assert(!dataAccess->hasBeenDiscounted());
				
				DataAccessStatusEffects initialStatus(dataAccess);
				dataAccess->setNewInstrumentationId(task->getInstrumentationTaskId());
				dataAccess->setInBottomMap();
				dataAccess->setRegistered();
#ifndef NDEBUG
				dataAccess->setReachable();
#endif
				DataAccessStatusEffects updatedStatus(dataAccess);
				
				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					dataAccess, accessStructures, task,
					hpDependencyData
				);
				
				// Unlock to avoid potential deadlock
				accessStructures._lock.unlock();
				
				replaceMatchingInBottomMapLinkAndPropagate(
					DataAccessLink(task, access_type), accessStructures,
					dataAccess->getAccessRegion(),
					parent, parentAccessStructures,
					hpDependencyData
				);
				
				// Relock to advance the iterator
				accessStructures._lock.lock();
				
				return true;
			}
		);
	}
	
	
	static inline void finalizeFragments(
		Task *task, TaskDataAccesses &accessStructures,
		/* OUT */ CPUDependencyData &hpDependencyData
	) {
		assert(task != nullptr);
		assert(!accessStructures.hasBeenDeleted());
		
		// Mark the fragments as completed and propagate topmost property
		accessStructures._accessFragments.processAll(
			[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
				DataAccess *fragment = &(*position);
				assert(fragment != nullptr);
				assert(!fragment->hasBeenDiscounted());
				
				// The fragment can be already complete due to the use of the "release" directive
				if (fragment->complete()) {
					return true;
				}
				
				DataAccessStatusEffects initialStatus(fragment);
				fragment->setComplete();
				DataAccessStatusEffects updatedStatus(fragment);
				
				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					fragment, accessStructures, task,
					hpDependencyData
				);
				
				return true;
			}
		);
	}
	
	
	template <typename ProcessorType>
	static inline void applyToAccessAndFragments(
		DataAccess *dataAccess, DataAccessRegion const &region,
		TaskDataAccesses &accessStructures,
		ProcessorType processor
	) {
		// Fragment if necessary
		dataAccess = fragmentAccess(dataAccess, region, accessStructures);
		assert(dataAccess != nullptr);
		
		bool hasSubaccesses = dataAccess->hasSubaccesses();
		DataAccessRegion finalRegion = dataAccess->getAccessRegion();
		bool alsoSubaccesses = processor(dataAccess);
		
		if (alsoSubaccesses && hasSubaccesses) {
			accessStructures._accessFragments.processIntersecting(
				finalRegion,
				[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
					DataAccess *fragment = &(*position);
					assert(fragment != nullptr);
					assert(!fragment->hasBeenDiscounted());
					
					fragment = fragmentAccess(fragment, finalRegion, accessStructures);
					assert(fragment != nullptr);
					
					processor(fragment);
					
					return true;
				}
			);
		}
	}
	
	
	static inline void finalizeAccess(
		Task *finishedTask, DataAccess *dataAccess, DataAccessRegion region,
		/* OUT */ CPUDependencyData &hpDependencyData
	) {
		assert(finishedTask != nullptr);
		assert(dataAccess != nullptr);
		
		assert(dataAccess->getOriginator() == finishedTask);
		assert(!region.empty());
		
		// The access may already have been released through the "release" directive
		if (dataAccess->complete()) {
			return;
		}
		assert(!dataAccess->hasBeenDiscounted());
		
		applyToAccessAndFragments(
			dataAccess, region,
			finishedTask->getDataAccesses(),
			[&] (DataAccess *accessOrFragment) -> bool {
				assert(!accessOrFragment->complete());
				assert(accessOrFragment->getOriginator() == finishedTask);
				
				DataAccessStatusEffects initialStatus(accessOrFragment);
				accessOrFragment->setComplete();
				DataAccessStatusEffects updatedStatus(accessOrFragment);
				
				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					accessOrFragment, finishedTask->getDataAccesses(), finishedTask,
					hpDependencyData
				);
				
				return true; // Apply also to subaccesses if any
			}
		);
	}
	
	
	static void handleRemovableTasks(
		/* inout */ CPUDependencyData::removable_task_list_t &removableTasks,
		ComputePlace *computePlace
	) {
		for (Task *removableTask : removableTasks) {
			TaskFinalization::disposeOrUnblockTask(removableTask, computePlace);
		}
		removableTasks.clear();
	}
	
	
	static void createTaskwait(
		Task *task, TaskDataAccesses &accessStructures, /* OUT */ CPUDependencyData &hpDependencyData)
	{
		if (accessStructures._subaccessBottomMap.empty()) {
			return;
		}
		
		// The last taskwait fragment will decreate the removal blocking count.
		// This is necessary to force the task to wait until all taskwait fragments have finished.
		task->increaseRemovalBlockingCount();
		
		// For each bottom map entry
		assert(accessStructures._taskwaitFragments.empty());
		accessStructures._subaccessBottomMap.processAll(
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);
				
				DataAccessLink previous = bottomMapEntry->_link;
				DataAccessRegion region = bottomMapEntry->_region;
				DataAccessType accessType = bottomMapEntry->_accessType;
				
				// Create the taskwait fragment
				{
					DataAccess *taskwaitFragment = createAccess(
						task,
						taskwait_type,
						accessType, /* not weak */ false, region,
						no_reduction_type_and_operator 
					);

					// No need for symbols in a taskwait
					
					DataAccessStatusEffects initialStatus(taskwaitFragment);
					taskwaitFragment->setNewInstrumentationId(task->getInstrumentationTaskId());
					taskwaitFragment->setInBottomMap();
					taskwaitFragment->setRegistered();
					
					// NOTE: For now we create it as completed, but we could actually link
					// that part of the status to any other actions that needed to be carried
					// out. For instance, data transfers.
					taskwaitFragment->setComplete();
#ifndef NDEBUG
					taskwaitFragment->setReachable();
#endif
					accessStructures._taskwaitFragments.insert(*taskwaitFragment);
					
					// Update the bottom map entry
					bottomMapEntry->_link._objectType = taskwait_type;
					bottomMapEntry->_link._task = task;
					
					DataAccessStatusEffects updatedStatus(taskwaitFragment);
					
					handleDataAccessStatusChanges(
						initialStatus, updatedStatus,
						taskwaitFragment, accessStructures, task,
						hpDependencyData
					);
				}
				
				TaskDataAccesses &previousAccessStructures = previous._task->getDataAccesses();
				
				// Unlock to avoid potential deadlock
				if (previous._task != task) {
					accessStructures._lock.unlock();
					previousAccessStructures._lock.lock();
				}
				
				followLink(
					previous, region,
					[&](DataAccess *previousAccess) -> bool
					{
						// Link to the taskwait
						DataAccessStatusEffects initialStatus(previousAccess);
						previousAccess->setNext(DataAccessLink(task, taskwait_type));
						previousAccess->unsetInBottomMap();
						DataAccessStatusEffects updatedStatus(previousAccess);
						
						handleDataAccessStatusChanges(
							initialStatus, updatedStatus,
							previousAccess, previousAccessStructures, previous._task,
							hpDependencyData
						);
						
						return true;
					}
				);
				
				// Relock to advance the iterator
				if (previous._task != task) {
					previousAccessStructures._lock.unlock();
					accessStructures._lock.lock();
				}
				
				return true;
			}
		);
	}
	
	
	static void createTopLevelSink(
		Task *task, TaskDataAccesses &accessStructures, /* OUT */ CPUDependencyData &hpDependencyData)
	{
		// For each bottom map entry
		accessStructures._subaccessBottomMap.processAll(
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);
				
				if (bottomMapEntry->_accessType != NO_ACCESS_TYPE) {
					// Not a local access
					return true;
				}
				
				DataAccessLink previous = bottomMapEntry->_link;
				DataAccessRegion region = bottomMapEntry->_region;
				DataAccessType accessType = bottomMapEntry->_accessType;
				
				// Create the top level sink fragment
				{
					DataAccess *topLevelSinkFragment = createAccess(
						task,
						top_level_sink_type,
						accessType, /* not weak */ false, region,
						no_reduction_type_and_operator 
					);

					// TODO, what is this? How to threat symbols?
					
					DataAccessStatusEffects initialStatus(topLevelSinkFragment);
					topLevelSinkFragment->setNewInstrumentationId(task->getInstrumentationTaskId());
					topLevelSinkFragment->setInBottomMap();
					topLevelSinkFragment->setRegistered();
					
					// NOTE: For now we create it as completed, but we could actually link
					// that part of the status to any other actions that needed to be carried
					// out. For instance, data transfers.
					topLevelSinkFragment->setComplete();
#ifndef NDEBUG
					topLevelSinkFragment->setReachable();
#endif
					accessStructures._taskwaitFragments.insert(*topLevelSinkFragment);
					
					// Update the bottom map entry
					bottomMapEntry->_link._objectType = top_level_sink_type;
					bottomMapEntry->_link._task = task;
					
					DataAccessStatusEffects updatedStatus(topLevelSinkFragment);
					
					handleDataAccessStatusChanges(
						initialStatus, updatedStatus,
						topLevelSinkFragment, accessStructures, task,
						hpDependencyData
					);
				}
				
				TaskDataAccesses &previousAccessStructures = previous._task->getDataAccesses();
				
				// Unlock to avoid potential deadlock
				if (previous._task != task) {
					accessStructures._lock.unlock();
					previousAccessStructures._lock.lock();
				}
				
				followLink(
					previous, region,
					[&](DataAccess *previousAccess) -> bool
					{
						// Link to the taskwait
						DataAccessStatusEffects initialStatus(previousAccess);
						previousAccess->setNext(DataAccessLink(task, taskwait_type));
						previousAccess->unsetInBottomMap();
						DataAccessStatusEffects updatedStatus(previousAccess);
						
						handleDataAccessStatusChanges(
							initialStatus, updatedStatus,
							previousAccess, previousAccessStructures, previous._task,
							hpDependencyData
						);
						
						return true;
					}
				);
				
				// Relock to advance the iterator
				if (previous._task != task) {
					previousAccessStructures._lock.unlock();
					accessStructures._lock.lock();
				}
				
				return true;
			}
		);
	}
	
	
	void registerTaskDataAccess(
		Task *task, DataAccessType accessType, bool weak, DataAccessRegion region, int symbolIndex, reduction_type_and_operator_index_t reductionTypeAndOperatorIndex
	) {
		assert(task != nullptr);
		
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());
		accessStructures._accesses.fragmentIntersecting(
			region,
			[&](DataAccess const &toBeDuplicated) -> DataAccess * {
				assert(!toBeDuplicated.isRegistered());
				return duplicateDataAccess(toBeDuplicated, accessStructures);
			},
			[](DataAccess *, DataAccess *) {}
		);
		
		accessStructures._accesses.processIntersectingAndMissing(
			region,
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *oldAccess = &(*position);
				assert(oldAccess != nullptr);
				
				upgradeAccess(oldAccess, accessType, weak, reductionTypeAndOperatorIndex);
				
				return true;
			},
			[&](DataAccessRegion missingRegion) -> bool {
				DataAccess *newAccess = createAccess(task, access_type, accessType, weak, missingRegion, reductionTypeAndOperatorIndex);
				newAccess->setSymbol(symbolIndex);			
	
				accessStructures._accesses.insert(*newAccess);
				
				return true;
			}
		);
	}
	
	
	bool registerTaskDataAccesses(
		Task *task,
		ComputePlace *computePlace
	) {
		assert(task != 0);
		assert(computePlace != nullptr);
		
		nanos_task_info *taskInfo = task->getTaskInfo();
		assert(taskInfo != 0);
		
		// This part creates the DataAccesses and calculates any possible upgrade
		taskInfo->register_depinfo(task, task->getArgsBlock());
		
		if (!task->getDataAccesses()._accesses.empty()) {
			task->increasePredecessors(2);
			
			CPUDependencyData &hpDependencyData = computePlace->getDependencyData();
#ifndef NDEBUG
			{
				bool alreadyTaken = false;
				assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, true));
			}
#endif
			
			// This part actually inserts the accesses into the dependency system
			linkTaskAccesses(hpDependencyData, task);
			
			processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(hpDependencyData, computePlace, true);
			
#ifndef NDEBUG
			{
				bool alreadyTaken = true;
				assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, false));
			}
#endif
			
			return task->decreasePredecessors(2);
		} else {
			return true;
		}
	}
	
	
	void releaseAccessRegion(
		Task *task, DataAccessRegion region,
		__attribute__((unused)) DataAccessType accessType, __attribute__((unused)) bool weak,
		ComputePlace *computePlace
	) {
		assert(task != nullptr);
		assert(computePlace != nullptr);
		
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());
		TaskDataAccesses::accesses_t &accesses = accessStructures._accesses;
		
		CPUDependencyData &hpDependencyData = computePlace->getDependencyData();
		
#ifndef NDEBUG
		{
			bool alreadyTaken = false;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, true));
		}
#endif
		
		{
			std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);
			
			accesses.processIntersecting(
				region,
				[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
					DataAccess *dataAccess = &(*position);
					assert(dataAccess != nullptr);
					assert(dataAccess->getType() == accessType);
					assert(dataAccess->isWeak() == weak);
					
					finalizeAccess(task, dataAccess, region, /* OUT */ hpDependencyData);
					
					return true;
				}
			);
		}
		processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(hpDependencyData, computePlace, true);
		
#ifndef NDEBUG
		{
			bool alreadyTaken = true;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, false));
		}
#endif
	}
	
	
	
	void unregisterTaskDataAccesses(Task *task, ComputePlace *computePlace)
	{
		assert(task != nullptr);
		assert(computePlace != nullptr);
		
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());
		TaskDataAccesses::accesses_t &accesses = accessStructures._accesses;
		
		CPUDependencyData &hpDependencyData = computePlace->getDependencyData();
		
#ifndef NDEBUG
		{
			bool alreadyTaken = false;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, true));
		}
#endif
		
		{
			std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);
			
			createTopLevelSink(task, accessStructures, hpDependencyData);
			
			accesses.processAll(
				[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
					DataAccess *dataAccess = &(*position);
					assert(dataAccess != nullptr);
					
					finalizeAccess(task, dataAccess, dataAccess->getAccessRegion(), /* OUT */ hpDependencyData);
					
					return true;
				}
			);
		}
		
		processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(hpDependencyData, computePlace, false);
		
#ifndef NDEBUG
		{
			bool alreadyTaken = true;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, false));
		}
#endif
	}
	
	
	void handleEnterBlocking(Task *task)
	{
		assert(task != nullptr);
		
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());
		std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);
		if (!accessStructures._accesses.empty()) {
			task->decreaseRemovalBlockingCount();
		}
	}
	
	
	void handleExitBlocking(Task *task)
	{
		assert(task != nullptr);
		
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());
		std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);
		if (!accessStructures._accesses.empty()) {
			task->increaseRemovalBlockingCount();
		}
	}
	
	
	void handleEnterTaskwait(Task *task, ComputePlace *computePlace)
	{
		assert(task != nullptr);
		assert(computePlace != nullptr);
		
		CPUDependencyData &hpDependencyData = computePlace->getDependencyData();
		
#ifndef NDEBUG
		{
			bool alreadyTaken = false;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, true));
		}
#endif
		
		{
			TaskDataAccesses &accessStructures = task->getDataAccesses();
			assert(!accessStructures.hasBeenDeleted());
			std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);
			if (!accessStructures._accesses.empty()) {
				assert(accessStructures._removalBlockers > 0);
				task->decreaseRemovalBlockingCount();
			}
			
			createTaskwait(task, accessStructures, hpDependencyData);
			
			finalizeFragments(task, accessStructures, hpDependencyData);
		}
		processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(hpDependencyData, computePlace, true);
		
#ifndef NDEBUG
		{
			bool alreadyTaken = true;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, false));
		}
#endif
	}
	
	
	void handleExitTaskwait(Task *task, __attribute__((unused)) ComputePlace *computePlace)
	{
		assert(task != nullptr);
		
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());
		std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);
		
		if (!accessStructures._accesses.empty()) {
			assert(accessStructures._removalBlockers > 0);
			task->increaseRemovalBlockingCount();
			
			// Mark all accesses as not having subaccesses
			accessStructures._accesses.processAll(
				[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
					DataAccess *dataAccess = &(*position);
					assert(dataAccess != nullptr);
					assert(!dataAccess->hasBeenDiscounted());
					
					if (dataAccess->hasSubaccesses()) {
						dataAccess->unsetHasSubaccesses();
					}
					
					return true;
				}
			);
			
			// Delete all fragments
			accessStructures._accessFragments.processAll(
				[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
					DataAccess *dataAccess = &(*position);
					assert(dataAccess != nullptr);
					
#ifndef NDEBUG
					DataAccessStatusEffects currentStatus(dataAccess);
					assert(currentStatus._isRemovable);
#endif
					
					Instrument::removedDataAccess(dataAccess->getInstrumentationId());
					accessStructures._accessFragments.erase(dataAccess);
					delete dataAccess;
					
					return true;
				}
			);
			accessStructures._accessFragments.clear();
			
			// Delete all taskwait fragments
			accessStructures._taskwaitFragments.processAll(
				[&](TaskDataAccesses::taskwait_fragments_t::iterator position) -> bool {
					DataAccess *dataAccess = &(*position);
					assert(dataAccess != nullptr);
					
#ifndef NDEBUG
					DataAccessStatusEffects currentStatus(dataAccess);
					assert(currentStatus._isRemovable);
#endif
					
					Instrument::removedDataAccess(dataAccess->getInstrumentationId());
					accessStructures._taskwaitFragments.erase(dataAccess);
					delete dataAccess;
					
					return true;
				}
			);
			accessStructures._taskwaitFragments.clear();
		}
		
		// Clean up the bottom map
		accessStructures._subaccessBottomMap.processAll(
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);
				assert((bottomMapEntry->_link._objectType == taskwait_type) || (bottomMapEntry->_link._objectType == top_level_sink_type));
				
				accessStructures._subaccessBottomMap.erase(bottomMapEntry);
				delete bottomMapEntry;
				
				return true;
			}
		);
		assert(accessStructures._subaccessBottomMap.empty());
	}
	
};

#pragma GCC visibility pop

