/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef EXECUTION_WORKFLOW_HPP
#define EXECUTION_WORKFLOW_HPP

#include "ExecutionStep.hpp"

class Task;
class ComputePlace;
class MemoryPlace;
class CPUDependencyData;

namespace ExecutionWorkflow {

	class Workflow {
	};

	void executeTask(Task *task, ComputePlace *targetComputePlace, MemoryPlace *targetMemoryPlace);

	void setupTaskwaitWorkflow(
		Task *task,
		DataAccess *taskwaitFragment,
		CPUDependencyData &hpDependencyData
	);
}

#endif /* EXECUTION_WORKFLOW_HPP */
