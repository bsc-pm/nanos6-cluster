/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef __EXECUTION_WORKFLOW_HPP__
#define __EXECUTION_WORKFLOW_HPP__

#include <functional>
#include <mutex>
#include <vector>

#include "ExecutionStep.hpp"
#include "CPUDependencyData.hpp"

class ComputePlace;
struct DataAccess;
class MemoryPlace;
class Task;

namespace ExecutionWorkflow {

	//! A function that sets up a data transfer between two MemoryPlace
	typedef std::function<Step *(MemoryPlace const *, MemoryPlace const *,
		DataAccessRegion const &, DataAccess *)> data_transfer_function_t;

	//! A map that stores the functions that perform data transfers between
	//! two MemoryPlaces, depending on their type (nanos6_device_t).
	typedef std::array<std::array<data_transfer_function_t, 4>, 4> transfers_map_t;

	inline Step *nullCopy(
		__attribute__((unused))MemoryPlace const *source,
		__attribute__((unused))MemoryPlace const *target,
		__attribute__((unused))DataAccessRegion const &translation,
		__attribute__((unused))DataAccess *access
	) {
		return new Step();
	}

	extern transfers_map_t _transfersMap;

	class Workflow {
		//! Root steps of the workflow
		std::vector<Step *> _rootSteps;

	public:

		//! \brief Creates a DataCopyStep.
		//!
		//! A DataCopyStep copies (if necessary) the (host-addressed)
		//! region from the sourceMemoryPlace to the targetMemoryPlace,
		//! using the targetTranslation for the target.
		//!
		//! \param[in] sourceMemoryPlace points to the MemoryPlace from which
		//!	       need to fetch data. This can be NULL in cases in which
		//!	       the location is not known yet, e.g. the copy step
		//!	       corresponds to a weak access.
		//! \param[in] targetMemoryPlace points to the MemoryPlace to which
		//!	       need to copy data to. This cannot be NULL.
		//! \param[in] region is the memory region to copy.
		//! \param[in] access is the DataAccess to which this copy step relates.
		Step *createDataCopyStep(
			MemoryPlace const *sourceMemoryPlace,
			MemoryPlace const *targetMemoryPlace,
			DataAccessRegion const &region,
			DataAccess *access,
			bool isTaskwait
		);

		//! \brief Creates an ExecutionStep.
		//!
		//! An ExecutionStep executes the task on assigned computePlace.
		//!
		//! \param[in] task is the Task for which we build the execution step
		//! \param[in] computePlace is the ComputePlace on which the task will
		//!	       be executed.
		Step *createExecutionStep(Task *task, ComputePlace *computePlace);

		//! \brief Creates a NotificationStep.
		//!
		//! A NotificationStep performs the cleanup of Task after
		//! it has finished executing and notify anyone who is waiting
		//! for the Task to complete.
		//!
		//! \param[in] callback is a function to be called once the notification
		//!	       Step becomes ready.
		//! \param[in] computePlace is the ComputePlace on which the task will
		//!	       be executed.
		Step *createNotificationStep(
			std::function<void ()> const &callback,
			ComputePlace const *computePlace
		);

		//! \brief Creates a DataReleaseStep.
		//!
		//! A DataReleaseStep triggers events related to the release
		//! of regions of a DataAccess
		//!
		//! \param[in] task is the Task for which we release an access.
		//! \param[in] access is the DataAccess that we are releasing.
		DataReleaseStep *createDataReleaseStep(Task *task);

		// \brief Enforces order between two steps of the Task execution.
		//
		// Create execute-after relationship between the two Steps of the workflow
		// so that the 'successor' Step will only start after the 'predecessor'
		// Step has completed.
		inline void enforceOrder(Step *predecessor, Step *successor)
		{
			if (predecessor == nullptr || successor == nullptr) {
				return;
			}

			predecessor->addSuccessor(successor);
			successor->addPredecessor();
		}

		//! \brief Add a root step to the Workflow.
		//!
		//! Root steps of the workflow are those Steps that do not have
		//! any predecessor. When the Workflow starts executing,
		//! essentially fires the execution of those Steps.
		inline void addRootStep(Step *root)
		{
			_rootSteps.push_back(root);
		}

		//! \brief Starts the execution of the workflow.
		//!
		//! This will start the execution of the root steps of the workflow.
		//! Whether the execution of the workflow has been completed when this
		//! method returns, depends on the nature of the steps from which the
		//! workflow consists of.
		void start();

	};

	//! \brief Create a workflow for executing a task
	//!
	//! \param[in] task is the Task we want to execute
	//! \param[in] targetComputePlace is the ComputePlace that the Scheduler decided
	//!            to execute the Task on.
	//! \param[in] targetMemoryPlace is the memory place that will be used for the
	//!            execution of the task, i.e. a MemoryPlace that is directly
	//!            accessible by targetComputePlace.
	void executeTask(Task *task, ComputePlace *targetComputePlace, MemoryPlace *targetMemoryPlace);

	//! \brief Creates a workflow for handling taskwaits
	//!
	//! \param[in] task is the Task to which the taskwait fragment belongs to
	//! \param[in] taskwaitFragment is the taskwait fragment for which we setup the workflow
	void setupTaskwaitWorkflow(Task *task, DataAccess *taskwaitFragment, CPUDependencyData &hpDependencyData);
};


#endif /* __EXECUTION_WORKFLOW_HPP__ */
