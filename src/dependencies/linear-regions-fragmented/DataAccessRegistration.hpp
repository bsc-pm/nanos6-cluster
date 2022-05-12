/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef DATA_ACCESS_REGISTRATION_HPP
#define DATA_ACCESS_REGISTRATION_HPP

#include <functional>

#include <api/nanos6/task-instantiation.h>
#include <DataAccessRegion.hpp>

#include "CPUDependencyData.hpp"
#include "../DataAccessType.hpp"
#include "ReductionSpecific.hpp"

class ClusterNode;
class ComputePlace;
class Task;


namespace DataAccessRegistration {
	//! \brief creates a task data access taking into account repeated accesses but does not link it to previous accesses nor superaccesses
	//!
	//! \param[in,out] task the task that performs the access
	//! \param[in] accessType the type of access
	//! \param[in] weak true iff the access is weak
	//! \param[in] region the region of data covered by the access
	//! \param[in] reductionTypeAndOperatorIndex an index that identifies the type and the operation of the reduction
	//! \param[in] reductionIndex an index that identifies the reduction within the task
	void registerTaskDataAccess(
		Task *task,
		DataAccessType accessType,
		bool weak,
		DataAccessRegion region,
		int symbolIndex,
		reduction_type_and_operator_index_t reductionTypeAndOperatorIndex,
		reduction_index_t reductionIndex
	);

	//! \brief Performs the task dependency registration procedure
	//!
	//! \param[in] task the Task whose dependencies need to be calculated
	//!
	//! \returns true if the task is already ready
	bool registerTaskDataAccesses(
		Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &dependencyData
	);


	void processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
		CPUDependencyData &hpDependencyData,
		ComputePlace *computePlace,
		bool fromBusyThread
	);

	void releaseAccessRegion(
		Task *task, DataAccessRegion region,
		DataAccessType accessType,
		bool weak,
		ComputePlace *computePlace,
		CPUDependencyData &dependencyData,
		WriteID writeID = 0,
		MemoryPlace const *location = nullptr,
		bool specifyingDependency = true
	);

	void unregisterLocallyPropagatedTaskDataAccesses(
		Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &dependencyData
	);

	void unregisterTaskDataAccesses1(
		Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &dependencyData,
		MemoryPlace *location,
		bool fromBusyThread
	);

	void unregisterTaskDataAccesses2(
		ComputePlace *computePlace,
		CPUDependencyData &dependencyData,
		MemoryPlace *location,
		bool fromBusyThread
	);

	inline void unregisterTaskDataAccesses(
		Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &dependencyData,
		MemoryPlace *location = nullptr,
		bool fromBusyThread = false,
		std::function<void()> callback = nullptr
	) {
		unregisterTaskDataAccesses1(task, computePlace, dependencyData, location, fromBusyThread);
		if (callback) {
			callback();
		}
		unregisterTaskDataAccesses2(computePlace, dependencyData, location, fromBusyThread);
	}

	//! \brief Combines the task reductions without releasing the dependencies
	//!
	//! \param[in] task the Task whose reductions need to be combined
	//! \param[in] computePlace the ComputePlace assigned to the current thread and where the task has been executed
	void combineTaskReductions(Task *task, ComputePlace *computePlace);

	//! \brief propagates satisfiability for an access.
	//!
	//! \param[in] task is the Task that includes the access for which we propagate.
	//! \param[in] region is the region for which propagate satisfiability
	//! \param[in] computePlace is the ComputePlace assigned to the current thread, or nullptr if none assigned
	//! \param[in] dependencyData is the CPUDependencyData struct used for the propagation operation.
	//! \param[in] readSatisfied is true if the region becomes read satisfied.
	//! \param[in] writeSatisfied is true if the region becomes write satisfied.
	//! \param[in] location is not a nullptr if we have an update for the location of the region.
#ifdef USE_CLUSTER
	void propagateSatisfiability(
		Task *task,
		DataAccessRegion const &region,
		ComputePlace *computePlace,
		CPUDependencyData &dependencyData,
		bool readSatisfied,
		bool writeSatisfied,
		WriteID writeID,
		MemoryPlace const *location,
		OffloadedTaskIdManager::OffloadedTaskId namespacePredecessor
	);

	void setNamespacePredecessor(
		Task *task,
		Task *parentTask,
		DataAccessRegion region,
		ClusterNode *remoteNode,
		OffloadedTaskIdManager::OffloadedTaskId namespacePredecessor
	);
#endif // USE_CLUSTER

	void handleEnterTaskwait(Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &dependencyData,
		bool noflush=false,
		bool nonLocalOnly=false);

	void handleExitTaskwait(Task *task, ComputePlace *computePlace, CPUDependencyData &dependencyData);

	//! \brief Mark a Taskwait fragment as completed
	//!
	//! \param[in] task is the Task that created the taskwait fragment
	//! \param[in] region is the taskwait region that has been completed
	//! \param[in] computePlace is the current ComputePlace of the caller
	//! \param[in] hpDependencyData is the CPUDependencyData used for delayed operations
	void releaseTaskwaitFragment(
		Task *task,
		DataAccessRegion region,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData,
		bool doDelayedOperations
	);

	//! \brief Pass all data accesses from the task through a lambda
	//!
	//! \param[in] task the owner of the accesses to be processed
	//! \param[in] processor a lambda that receives a reference to the access
	//!
	//! \returns false if the traversal was stopped before finishing
	template <typename ProcessorType>
	inline bool processAllDataAccesses(Task *task, ProcessorType processor);

	void processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
		CPUDependencyData &hpDependencyData,
		ComputePlace *computePlace,
		bool fromBusyThread);

	//! \brief Update the location of the DataAccess of a Task
	//!
	//! \param[in] task is the owner of the accesses we are updating
	//! \param[in] region is the DataAccessRegion of which the location we are updating
	//! \param[in] location is the new location of the DataAccess
	//! \param[in] isTaskwait is true if the update refers to a taskwait object
	void updateTaskDataAccessLocation(
		Task *task,
		DataAccessRegion const &region,
		MemoryPlace const *location,
		bool isTaskwait
	);

	void setLocationFromWorkflow(
		DataAccess *access,
		MemoryPlace const *location,
		CPUDependencyData &hpDependencyData
	);

	//! \brief Register a region as a NO_ACCESS_TYPE access within the Task
	//!
	//! This is meant to be used for registering a new DataAccess that
	//! represents a new memory allocation in the context of a task. This
	//! way we can keep track of the location of that region in situations
	//! that we loose all information about it, e.g. after a taskwait
	//!
	//! \param[in] task is the Task that registers the access region
	//! \param[in] region is the DataAccessRegion being registered
	void registerLocalAccess(Task *task, DataAccessRegion const &region, const MemoryPlace *location, bool isStack);

	//! \brief Unregister a local region from the accesses of the Task
	//!
	//! This is meant to be used for unregistering a DataAccess with
	//! NO_ACCESS_TYPE that was previously registered calling
	//! 'registerLocalAccess', when we are done with this region, i.e. the
	//! corresponging memory has been deallocated.
	//!
	//! \param[in] task is the Task that region is registerd to
	//! \param[in] region is the DataAccessRegion being unregistered
	void unregisterLocalAccess(Task *task, DataAccessRegion const &region, bool isStack);

	//! \brief Generate the symbol translation table for reductions
	//!
	//! \param[in] task is the Task to be executed
	//! \param[in] computePlace is where it will be executed
	//! \param[in] translationTable is the symbol table to use
	//! \param[in] totalSymbols is the number of rows in the translation table
	void translateReductionAddresses(Task *task, ComputePlace *computePlace,
		nanos6_address_translation_entry_t * translationTable, int totalSymbols);

	void setNamespaceSelf(DataAccess *access, int targetNamespace, CPUDependencyData &hpDependencyData);

	typedef std::vector<DataAccessRegion> DataAccessRegionVector;
	void checkNamespacePropagation(DataAccessRegionVector regions, Task *offloadedTask);

	void noEagerSend(Task *task, DataAccessRegion region);

	bool supportsDataTracking();
} // namespace DataAccessRegistration


#endif // DATA_ACCESS_REGISTRATION_HPP
