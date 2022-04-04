/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#include "ExecutionWorkflowHost.hpp"
#include "dependencies/SymbolTranslation.hpp"
#include "executors/threads/TaskFinalization.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "hardware/places/ComputePlace.hpp"
#include "hardware/places/MemoryPlace.hpp"
#include "scheduling/Scheduler.hpp"
#include "system/TrackingPoints.hpp"
#include "system/If0Task.hpp"
#include "tasks/Task.hpp"
#include "tasks/Taskfor.hpp"

#include <DataAccessRegistration.hpp>
#include <InstrumentInstrumentationContext.hpp>
#include <InstrumentThreadInstrumentationContext.hpp>
#include "cluster/hybrid/ClusterStats.hpp"


namespace ExecutionWorkflow {

	void HostExecutionStep::start()
	{
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		CPU *cpu = (currentThread == nullptr) ? nullptr : currentThread->getComputePlace();
		Task *currentTask = (currentThread == nullptr) ? nullptr : currentThread->getTask();

		//! We are trying to start the execution of the Task from within
		//! something that is not a WorkerThread, or it does not have
		//! a CPU or the task assigned to it.
		//!
		//! This will happen once the last DataCopyStep finishes and
		//! releases the ExecutionStep.
		//!
		//! In that case we need to add the Task back for scheduling.
		if (currentThread == nullptr
			|| cpu == nullptr
			|| currentTask == nullptr
			|| (currentTask->isPolling() && _task != currentTask)) {

			_task->setExecutionStep(this);
		}

		// We are trying to start the execution of the Task from within
		// something that is not a WorkerThread, or it does not have
		// a CPU or the task assigned to it
		//
		// This will happen once the last DataCopyStep finishes and
		// releases the ExecutionStep
		//
		// In that case we need to add the Task back for scheduling
		if ((cpu == nullptr) 
			|| (currentThread->getTask() == nullptr)
			|| (_task->isTaskforSource() && (_task->getExecutionStep() == nullptr))) {

			_task->setExecutionStep(this);
			Scheduler::addReadyTask(_task, nullptr, BUSY_COMPUTE_PLACE_TASK_HINT);

			return;
		}

		if (_task->isRunnable()) {
			_task->setThread(currentThread);

			if (_task->hasCode()) {
				const Instrument::task_id_t taskId
					= _task->isTaskforCollaborator()
					? _task->getParent()->getInstrumentationTaskId()
					: _task->getInstrumentationTaskId();

				Instrument::ThreadInstrumentationContext instrumentationContext(
					taskId,
					cpu->getInstrumentationId(),
					currentThread->getInstrumentationId()
				);

				nanos6_address_translation_entry_t
					stackTranslationTable[SymbolTranslation::MAX_STACK_SYMBOLS];

				size_t tableSize = 0;
				nanos6_address_translation_entry_t *translationTable =
					SymbolTranslation::generateTranslationTable(
						_task, cpu, stackTranslationTable, tableSize);

				ClusterStats::startTask(currentThread);

				// Runtime Tracking Point - A task starts its execution
				TrackingPoints::taskIsExecuting(_task);

				// Register the stack unless it is the node namespace.
				// We should not register the stack for the node namespace because
				// (1) The node namespace is part of the runtime, so not even
				//     an allmemory access should be sharing data with it. We
				//     would be pointlessly adding two accesses to every task with
				//     an all memory region.
				// (2) All memory accesses would then be subaccesses of the stack,
				//     creating fragments of the node namespace's stack in the
				//     node namespace. The node namespace normally never gets any
				//     fragments because it never normally has any accesses. Creating
				//     these fragments would create a load of problems, including
				//     having to recognise and delete them when an offloaded task
				//     completes and removes itself from the bottom map.
				size_t stackSize;
				void *stackPtr = currentThread->getStackAndSize(/* OUT */ stackSize);
				DataAccessRegion stackRegion(stackPtr, stackSize);
				if (!_task->isNodeNamespace()) {
					DataAccessRegistration::registerLocalAccess(_task, stackRegion, ClusterManager::getCurrentMemoryNode(), /* isStack */ true);
				}

				// Run the task
				std::atomic_thread_fence(std::memory_order_acquire);
				_task->body(translationTable);
				if (_task->isIf0()) {
					If0Task::executeNonInline(currentThread, _task, cpu);
				}
				std::atomic_thread_fence(std::memory_order_release);

				// Unregister the stack
				if (!_task->isNodeNamespace()) {
					DataAccessRegistration::unregisterLocalAccess(_task, stackRegion, /* isStack */ true);
				}

				// Update the CPU since the thread may have migrated
				cpu = currentThread->getComputePlace();
				instrumentationContext.updateComputePlace(cpu->getInstrumentationId());

				// Runtime Tracking Point - A task completes its execution (user code)
				TrackingPoints::taskCompletedUserCode(_task);

				// Free up all symbol translation
				if (tableSize > 0) {
					MemoryAllocator::free(translationTable, tableSize);
				}
				ClusterStats::endTask(currentThread);
			} else {

				if (_task->isIf0()) {
					If0Task::executeNonInline(currentThread, _task, cpu);
				}
				// Runtime Tracking Point - A task completes its execution (user code)
				TrackingPoints::taskCompletedUserCode(_task);
			}

			DataAccessRegistration::combineTaskReductions(_task, cpu);
		}

		if (_task->getCountedAsImmovable()) {
			ClusterHybridMetrics::incNumImmovableTasks(-1);
		}

		// Release the subsequent steps
		_task->setExecutionStep(nullptr);
		releaseSuccessors();
		delete this;
	}
}; // namespace ExecutionWorkflow
