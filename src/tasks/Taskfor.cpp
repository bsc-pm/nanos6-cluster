/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include "Taskfor.hpp"
#include "executors/threads/WorkerThread.hpp"
#include <InstrumentTaskExecution.hpp>

void Taskfor::run(Taskfor &source, nanos6_address_translation_entry_t *translationTable)
{
	assert(getParent()->isTaskfor() && getParent() == &source);
	assert(getMyChunk() >= 0);

	// Temporary hack in order to solve the problem of updating the location of the DataAccess
	// objects of the Taskfor, when we unregister them, until we solve this properly, by supporting
	// the Taskfor construct through the execution workflow
	CPU * const cpu = dynamic_cast<CPU*>(getThread()->getComputePlace());
	assert(cpu != nullptr);

	MemoryPlace * const memoryPlace = cpu->getMemoryPlace(0);
	source.setMemoryPlace(memoryPlace);

	// Compute source taskfor total chunks
	bounds_t const &sourceBounds = source.getBounds();
	const size_t totalIterations = sourceBounds.upper_bound - sourceBounds.lower_bound;
	const size_t totalChunks = MathSupport::ceil(totalIterations, sourceBounds.chunksize);

	// Get the arguments and the task information
	const nanos6_task_info_t &taskInfo = *getTaskInfo();
	void *argsBlock = getArgsBlock();
	size_t myIterations = computeChunkBounds(totalChunks, sourceBounds);
	assert(myIterations > 0);
	size_t completedIterations = 0;

	do {
		Instrument::taskforChunk(_myChunk);
		taskInfo.implementations[0].run(argsBlock, &_bounds, translationTable);
		Instrument::taskforChunk(-1);
		// Prevent translating twice the addresses because the argsBlock is overwritten
		translationTable = nullptr;

		completedIterations += myIterations;

		// Stop after one chunk, to give the scheduler the opportunity to interrupt this
		// taskfor and schedule a higher priority task. Note: this does somewhat increase
		// the overhead when each chunk is a tiny amount of work. The workaround is simply
		// to increase the chunksize. If it is ever a problem, we could (1) ask the
		// scheduler here if it has a higher priority task and stop if it does or (2)
		// stop and re-enter the scheduler after a fixed wallclock time that is large enough
		// to ensure that the re-scheduling overhead is manageable.
		break;

		// _myChunk = source.getNextChunk(cpu);
		// if (_myChunk >= 0) {
		// 	myIterations = computeChunkBounds(totalChunks, sourceBounds);
		// } else {
		// 	myIterations = 0;
		// }
	} while (myIterations != 0);

	assert(completedIterations > 0);
	assert(completedIterations <= source._bounds.upper_bound);
	_completedIterations = completedIterations;

	source.notifyCollaboratorHasFinished();
}
