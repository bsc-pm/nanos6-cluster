/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#include "ExecutionWorkflow.hpp"
#include "dependencies/SymbolTranslation.hpp"
#include "executors/threads/TaskFinalization.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "hardware/places/ComputePlace.hpp"
#include "hardware/places/MemoryPlace.hpp"
#include "system/TrackingPoints.hpp"
#include "tasks/Task.hpp"
#include "tasks/Taskfor.hpp"
#include "tasks/TaskImplementation.hpp"

#include <DataAccessRegistration.hpp>
#include <InstrumentInstrumentationContext.hpp>
#include <InstrumentThreadInstrumentationContext.hpp>
#include <InstrumentDependencySubsystemEntryPoints.hpp>

#include <InstrumentThreadManagement.hpp>
#include <Monitoring.hpp>
#include "cluster/hybrid/ClusterStats.hpp"


namespace ExecutionWorkflow {

	void executeTask(Task *task, ComputePlace *targetComputePlace, MemoryPlace *targetMemoryPlace)
	{
		nanos6_address_translation_entry_t stackTranslationTable[SymbolTranslation::MAX_STACK_SYMBOLS];

		CPU *cpu = (CPU *) targetComputePlace;
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		assert(currentThread != nullptr);
		assert(task != nullptr);

		task->setThread(currentThread);
		task->setMemoryPlace(targetMemoryPlace);

		bool isTaskforCollaborator = task->isTaskforCollaborator();
		Instrument::task_id_t taskId;
		if (isTaskforCollaborator) {
			taskId = task->getParent()->getInstrumentationTaskId();
		} else {
			taskId = task->getInstrumentationTaskId();
		}
		Instrument::ThreadInstrumentationContext instrumentationContext(
			taskId,
			cpu->getInstrumentationId(),
			currentThread->getInstrumentationId()
		);

		if (task->hasCode()) {
			size_t tableSize = 0;
			nanos6_address_translation_entry_t *translationTable =
				SymbolTranslation::generateTranslationTable(
					task, targetComputePlace,
					stackTranslationTable, tableSize);

			// Runtime Tracking Point - A task starts its execution
			TrackingPoints::taskIsExecuting(task);
			ClusterStats::startTask(currentThread);

			// Run the task
			std::atomic_thread_fence(std::memory_order_acquire);
			task->body(translationTable);
			std::atomic_thread_fence(std::memory_order_release);

			// Update the CPU since the thread may have migrated
			cpu = currentThread->getComputePlace();
			instrumentationContext.updateComputePlace(cpu->getInstrumentationId());

			// Runtime Tracking Point - A task has completed its execution (user code)
			TrackingPoints::taskCompletedUserCode(task);

			// Free up all symbol translation
			if (tableSize > 0) {
				MemoryAllocator::free(translationTable, tableSize);
			}
			ClusterStats::endTask(currentThread);
		} else {
			// Runtime Tracking Point - A task has completed its execution (user code)
			TrackingPoints::taskCompletedUserCode(task);
		}

		DataAccessRegistration::combineTaskReductions(task, cpu);

		if (task->markAsFinished(cpu)) {
			DataAccessRegistration::unregisterTaskDataAccesses(
				task, cpu, cpu->getDependencyData()
			);

			TaskFinalization::taskFinished(task, cpu);
			if (task->markAsReleased()) {
				TaskFinalization::disposeTask(task);
			}
		}
	}

	void setupTaskwaitWorkflow(Task *task, DataAccess *taskwaitFragment, CPUDependencyData &hpDependencyData)
	{
		Instrument::enterSetupTaskwaitWorkflow();
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();

		ComputePlace *computePlace =
			(currentThread != nullptr) ? currentThread->getComputePlace() : nullptr;

		DataAccessRegistration::releaseTaskwaitFragment(
			task, taskwaitFragment->getAccessRegion(),
			computePlace, hpDependencyData, false
		);
		Instrument::exitSetupTaskwaitWorkflow();
	}
}
