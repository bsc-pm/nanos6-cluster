/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef EXECUTION_WORKFLOW_HOST_HPP
#define EXECUTION_WORKFLOW_HOST_HPP

#include "ExecutionStep.hpp"
#include <functional>

class MemoryPlace;
class ComputePlace;
class Task;
struct DataAccess;

namespace ExecutionWorkflow {

	class HostExecutionStep : public Step {
		Task *_task;
		ComputePlace *_computePlace;
	public:
		HostExecutionStep(Task *task, ComputePlace *computePlace)
			: Step(), _task(task), _computePlace(computePlace)
		{
		}

		//! Start the execution of the Step
		void start() override;
	};

	class HostNotificationStep : public Step {
		std::function<void ()> const _callback;
	public:
		HostNotificationStep(std::function<void ()> const &callback)
			: _callback(callback)
		{
		}

		//! start the execution of the Step
		inline void start() override
		{
			_callback();
			releaseSuccessors();
			delete this;
		}
	};
};

#endif /* __EXECUTION_WORKFLOW_HOST_HPP__ */
