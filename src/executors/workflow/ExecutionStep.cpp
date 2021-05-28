/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "ExecutionStep.hpp"

#include <DataAccess.hpp>

#include <tasks/Task.hpp>

namespace ExecutionWorkflow {

	DataLinkStep::DataLinkStep(DataAccess *access)
		: Step(),
		/* We count twice the bytes of the region, because we
		 * need to link both for Read and Write satisfiability */
		_bytesToLink(2 * access->getAccessRegion().getSize())
	{
	}

	DataReleaseStep::DataReleaseStep(Task *task)
		: Step(),
		_task(task),
		_infoLock(task->_releaseStepInfoLock)
	{
	}

}
