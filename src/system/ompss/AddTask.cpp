/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

// This is for posix_memalign
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#endif

#include <cassert>
#include <cstdlib>

#include <nanos6.h>

#include "AddTask.hpp"
#include "MemoryAllocator.hpp"
#include "executors/threads/CPUManager.hpp"
#include "executors/threads/ThreadManager.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "hardware/HardwareInfo.hpp"
#include "hardware/places/ComputePlace.hpp"
#include "hardware-counters/HardwareCounters.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "scheduling/Scheduler.hpp"
#include "system/If0Task.hpp"
#include "tasks/StreamExecutor.hpp"
#include "tasks/Task.hpp"
#include "tasks/TaskImplementation.hpp"
#include "tasks/Taskfor.hpp"
#include "tasks/Taskloop.hpp"

#include <DataAccessRegistration.hpp>
#include <InstrumentAddTask.hpp>
#include <InstrumentTaskStatus.hpp>
#include <InstrumentThreadInstrumentationContext.hpp>
#include <Monitoring.hpp>
#include <TaskDataAccesses.hpp>
#include <TaskDataAccessesInfo.hpp>


#define DATA_ALIGNMENT_SIZE sizeof(void *)

void nanos6_create_task(
	nanos6_task_info_t *taskInfo,
	nanos6_task_invocation_info_t *taskInvocationInfo,
	size_t args_block_size,
	void **args_block_pointer,
	void **task_pointer,
	size_t flags,
	size_t num_deps
) {
	assert(taskInfo->implementation_count == 1); //TODO: Temporary check until multiple implementations are supported

	nanos6_device_t taskDeviceType = (nanos6_device_t) taskInfo->implementations[0].device_type_id;
	if (!HardwareInfo::canDeviceRunTasks(taskDeviceType)) {
		FatalErrorHandler::failIf(true, "Task of device type '", taskDeviceType, "' has no active hardware associated");
	}

	Instrument::task_id_t taskId = Instrument::enterAddTask(taskInfo, taskInvocationInfo, flags);

	WorkerThread *currentWorkerThread = WorkerThread::getCurrentWorkerThread();
	if (currentWorkerThread != nullptr) {
		Task *parent = currentWorkerThread->getTask();
		if (parent != nullptr) {
			HardwareCounters::taskStopped(parent);
			Monitoring::taskChangedStatus(parent, runtime_status);
		}
	}

	// Operate directly over references to the user side variables
	void *&args_block = *args_block_pointer;
	void *&task = *task_pointer;

	bool isTaskfor = flags & nanos6_task_flag_t::nanos6_taskfor_task;
	bool isTaskloop = flags & nanos6_task_flag_t::nanos6_taskloop_task;
	bool isStreamExecutor = flags & (1 << Task::stream_executor_flag);
	size_t originalArgsBlockSize = args_block_size;
	size_t taskSize;
	if (isTaskfor) {
		taskSize = sizeof(Taskfor);
	} else if (isStreamExecutor) {
		taskSize = sizeof(StreamExecutor);
	} else if (isTaskloop) {
		taskSize = sizeof(Taskloop);
	} else {
		taskSize = sizeof(Task);
	}

	TaskDataAccessesInfo taskAccessInfo(num_deps);

	// Get the size needed for this task's hardware counters
	size_t taskCountersSize = TaskHardwareCounters::getTaskHardwareCountersSize();

	bool hasPreallocatedArgsBlock = (flags & nanos6_preallocated_args_block);

	if (hasPreallocatedArgsBlock) {
		assert(args_block != nullptr);
		task = MemoryAllocator::alloc(taskSize + taskCountersSize);
	} else {
		// Alignment fixup
		size_t missalignment = args_block_size & (DATA_ALIGNMENT_SIZE - 1);
		size_t correction = (DATA_ALIGNMENT_SIZE - missalignment) & (DATA_ALIGNMENT_SIZE - 1);
		args_block_size += correction;

		// Allocation and layout
		*args_block_pointer = MemoryAllocator::alloc(args_block_size + taskSize + taskAccessInfo.getAllocationSize() + taskCountersSize);
		task = (char *)args_block + args_block_size;
	}

	Instrument::createdArgsBlock(taskId, *args_block_pointer, originalArgsBlockSize, args_block_size);

	taskAccessInfo.setAllocationAddress((char *)task + taskSize);

	// Prepare the allocation address for the task's hardware counters
	void *countersAddress = (taskCountersSize > 0) ? (char *)task + taskSize + taskAccessInfo.getAllocationSize() : nullptr;
	TaskHardwareCounters taskCounters(countersAddress);

	if (isTaskfor) {
		// Taskfor is always final.
		flags |= nanos6_task_flag_t::nanos6_final_task;
		new (task) Taskfor(args_block, originalArgsBlockSize, taskInfo, taskInvocationInfo, nullptr, taskId, flags, taskAccessInfo, taskCounters);
	} else if (isStreamExecutor) {
		new (task) StreamExecutor(args_block, originalArgsBlockSize, taskInfo, taskInvocationInfo, nullptr, taskId, flags, taskAccessInfo, taskCounters);
	} else if (isTaskloop) {
		new (task) Taskloop(args_block, originalArgsBlockSize, taskInfo, taskInvocationInfo, nullptr, taskId, flags, taskAccessInfo, taskCounters);
	} else {
		// Construct the Task object
		new (task) Task(args_block, originalArgsBlockSize, taskInfo, taskInvocationInfo, /* Delayed to the submit call */ nullptr, taskId, flags, taskAccessInfo, taskCounters);
	}
}

void nanos6_create_preallocated_task(
	nanos6_task_info_t *taskInfo,
	nanos6_task_invocation_info_t *taskInvocationInfo,
	Instrument::task_id_t parentTaskInstrumentationId,
	size_t args_block_size,
	void *preallocatedArgsBlock,
	void *preallocatedTask,
	size_t flags
) {
	assert(taskInfo->implementation_count == 1); //TODO: Temporary check until multiple implementations are supported
	assert(preallocatedArgsBlock != nullptr);
	assert(preallocatedTask != nullptr);

	Instrument::task_id_t taskId = Instrument::enterAddTaskforCollaborator(parentTaskInstrumentationId, taskInfo, taskInvocationInfo, flags);

	bool isTaskfor = flags & nanos6_task_flag_t::nanos6_taskfor_task;
	FatalErrorHandler::failIf(!isTaskfor, "Only taskfors can be created this way.");

	Taskfor *taskfor = (Taskfor *) preallocatedTask;
	taskfor->reinitialize(preallocatedArgsBlock, args_block_size, taskInfo, taskInvocationInfo, nullptr, taskId, flags);
}

void nanos6_submit_task(void *taskHandle)
{
	Task *task = (Task *) taskHandle;
	assert(task != nullptr);

	Instrument::task_id_t taskInstrumentationId = task->getInstrumentationTaskId();

	Task *parent = nullptr;
	WorkerThread *currentWorkerThread = WorkerThread::getCurrentWorkerThread();
	ComputePlace *computePlace = nullptr;

	//! A WorkerThread might spawn a remote task through a polling service,
	//! i.e. while not executing a Task already. So here, we need to check
	//! both, if we are running from inside a WorkerThread as well as if
	//! we are running a Task
	if (currentWorkerThread != nullptr && currentWorkerThread->getTask() != nullptr) {
		parent = currentWorkerThread->getTask();
		assert(parent != nullptr);

		computePlace = currentWorkerThread->getComputePlace();
		assert(computePlace != nullptr);

		task->setParent(parent);

		if (parent->isStreamExecutor()) {
			// Check if we need to save the spawned function's id for a future
			// trigger of a callback (spawned stream functions)
			StreamExecutor *executor = (StreamExecutor *) parent;
			StreamFunctionCallback *callback = executor->getCurrentFunctionCallback();

			if (callback != nullptr) {
				task->setParentSpawnCallback(callback);
				executor->increaseCallbackParticipants(callback);
			}
		}
	}

	HardwareCounters::taskCreated(task);
	Instrument::createdTask(task, taskInstrumentationId);
	Monitoring::taskCreated(task);

	// Compute the task priority only when the scheduler is
	// considering the task priorities
	if (Scheduler::isPriorityEnabled()) {
		if (task->computePriority()) {
			Instrument::taskHasNewPriority(
				task->getInstrumentationTaskId(),
				task->getPriority());
		}
	}

	bool ready = true;
	nanos6_task_info_t *taskInfo = task->getTaskInfo();
	assert(taskInfo != 0);
	if (taskInfo->register_depinfo != 0) {
		// Begin as pending status, become ready later, through the scheduler
		Instrument::ThreadInstrumentationContext instrumentationContext(taskInstrumentationId);
		Instrument::taskIsPending(taskInstrumentationId);

		Monitoring::taskChangedStatus(task, pending_status);
		// No need to stop hardware counters, as the task was created just now

		ready = DataAccessRegistration::registerTaskDataAccesses(task, computePlace, computePlace->getDependencyData());
	}
	assert(parent != nullptr || ready);

	bool isIf0 = task->isIf0();

	// We cannot execute inline tasks that are not to run in the host
	bool executesInDevice = (task->getDeviceType() != nanos6_host_device);

	if (ready && (!isIf0 || executesInDevice)) {
		// Queue the task if ready but not if0
		ReadyTaskHint schedulingHint = NO_HINT;

		if (currentWorkerThread != nullptr) {
			schedulingHint = CHILD_TASK_HINT;
		}

		Scheduler::addReadyTask(task, computePlace, schedulingHint);

		// After adding a task, the CPUManager may want to unidle CPUs
		CPUManager::executeCPUManagerPolicy(computePlace, ADDED_TASKS, 1);
	}

	if (parent != nullptr) {
		HardwareCounters::taskStarted(parent);
		Monitoring::taskChangedStatus(parent, executing_status);
	}

	Instrument::exitAddTask(taskInstrumentationId);

	// Special handling for if0 tasks
	if (isIf0) {
		if (ready && !executesInDevice) {
			// Ready if0 tasks are executed inline, if they are not device tasks
			If0Task::executeInline(currentWorkerThread, parent, task, computePlace);
		} else {
			// Non-ready if0 tasks cause this thread to get blocked
			If0Task::waitForIf0Task(currentWorkerThread, parent, task, computePlace);
		}
	}
}
