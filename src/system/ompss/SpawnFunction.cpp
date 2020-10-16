/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include <cassert>
#include <map>
#include <mutex>
#include <string>
#include <utility>

#include <nanos6.h>
#include <nanos6/library-mode.h>

#include "AddTask.hpp"
#include "SpawnFunction.hpp"
#include "lowlevel/SpinLock.hpp"
#include "system/TrackingPoints.hpp"
#include "tasks/StreamManager.hpp"
#include "tasks/Task.hpp"
#include "tasks/TaskInfo.hpp"

#include <InstrumentAddTask.hpp>


//! Static members
std::atomic<unsigned int> SpawnFunction::_pendingSpawnedFunctions(0);
std::map<SpawnFunction::task_info_key_t, nanos6_task_info_t> SpawnFunction::_spawnedFunctionInfos;
SpinLock SpawnFunction::_spawnedFunctionInfosLock;
nanos6_task_invocation_info_t SpawnFunction::_spawnedFunctionInvocationInfo = { "Spawned from external code" };

//! Args block of spawned functions
struct SpawnedFunctionArgsBlock {
	SpawnFunction::function_t _function = nullptr;
	void *_args = nullptr;
	SpawnFunction::function_t _completionCallback = nullptr;
	void *_completionArgs = nullptr;
	nanos6_task_constraints_t _constraints = { .cost = 0, .stream = 0, .node = DEFAULT_NODE_VALUE};

	void set(
		SpawnFunction::function_t function,
		void *args,
		SpawnFunction::function_t completion_callback,
		void *completion_args,
		size_t stream_id
	) {
		_function = function;
		_args = args;
		_completionCallback = completion_callback;
		_completionArgs = completion_args;
		_constraints.stream = stream_id;
	}
};


void SpawnFunction::spawnFunction(
	function_t function,
	void *args,
	function_t completionCallback,
	void *completionArgs,
	char const *label,
	bool fromUserCode,
	size_t extra_flag,
	size_t streamId
) {
	WorkerThread *workerThread = WorkerThread::getCurrentWorkerThread();
	Task *creator = nullptr;
	if (workerThread != nullptr) {
		creator = workerThread->getTask();
	}

	// Runtime Tracking Point - Entering the creation of a task
	TrackingPoints::enterSpawnFunction(creator, fromUserCode);

	nanos6_task_info_t *taskInfo = nullptr;

	{
		task_info_key_t taskInfoKey(function, (label != nullptr ? label : ""));

		std::lock_guard<SpinLock> guard(_spawnedFunctionInfosLock);

		auto itAndBool = _spawnedFunctionInfos.emplace(
			std::make_pair(taskInfoKey, nanos6_task_info_t())
		);

		auto it = itAndBool.first;
		taskInfo = &(it->second);

		if (itAndBool.second) {
			// New task info
			taskInfo->implementations = (nanos6_task_implementation_info_t *)
				malloc(sizeof(nanos6_task_implementation_info_t));

			taskInfo->implementation_count = 1;
			taskInfo->implementations[0].run = SpawnFunction::spawnedFunctionWrapper;
			taskInfo->implementations[0].device_type_id = nanos6_device_t::nanos6_host_device;
			taskInfo->register_depinfo = nullptr;

			// The completion callback will be called when the task is destroyed
			taskInfo->destroy_args_block = SpawnFunction::spawnedFunctionDestructor;

			// Use a copy since we do not know the actual lifetime of label
			taskInfo->implementations[0].task_label = it->first.second.c_str();
			taskInfo->implementations[0].declaration_source = "Spawned Task";

			if (streamId == 0) {  // This optimises some checks latter
				taskInfo->implementations[0].get_constraints = nullptr;
			} else {
				taskInfo->implementations[0].get_constraints
					= SpawnFunction::spawnedFunctionGetConstraints;
			}
		}

		assert ((streamId == 0 && taskInfo->implementations[0].get_constraints == nullptr)
			|| (streamId > 0 && taskInfo->implementations[0].get_constraints != nullptr));
	}

	// Register the new task info
	const bool newTaskType = TaskInfo::registerTaskInfo(taskInfo);
	if (newTaskType) {
		Instrument::registeredNewSpawnedTaskType(taskInfo);
	}

	const size_t flags = nanos6_waiting_task | extra_flag;

	// Create the task representing the spawned function
	Task *task = AddTask::createTask(
		taskInfo, &_spawnedFunctionInvocationInfo,
		nullptr, sizeof(SpawnedFunctionArgsBlock),
		flags
	);
	assert(task != nullptr);

	SpawnedFunctionArgsBlock *argsBlock = (SpawnedFunctionArgsBlock *) task->getArgsBlock();
	assert(argsBlock != nullptr);

	argsBlock->set(function, args, completionCallback, completionArgs, streamId);

	// Increase the number of spawned functions if it is not a polling
	if ((extra_flag & Task::nanos6_task_runtime_flag_t::nanos6_polling_flag) == 0) {
		_pendingSpawnedFunctions++;
	}

#ifdef EXTRAE_ENABLED
	if (label != nullptr && strcmp(label, "main") == 0) {
		task->markAsMainTask();
	}
#endif

	// Submit the task without parent
	AddTask::submitTask(task, nullptr);

	// Runtime Tracking Point - Exiting the creation of a task
	TrackingPoints::exitSpawnFunction(creator, fromUserCode);
}

void SpawnFunction::spawnedFunctionWrapper(void *args, void *, nanos6_address_translation_entry_t *)
{
	SpawnedFunctionArgsBlock *argsBlock = (SpawnedFunctionArgsBlock *) args;
	assert(argsBlock != nullptr);

	// Call the user spawned function
	argsBlock->_function(argsBlock->_args);
}

void SpawnFunction::spawnedFunctionGetConstraints(
	void *args,
	nanos6_task_constraints_t *const constraints
) {
	SpawnedFunctionArgsBlock *argsBlock = (SpawnedFunctionArgsBlock *) args;
	assert(argsBlock != nullptr);

	*constraints = argsBlock->_constraints;
}

void SpawnFunction::spawnedFunctionDestructor(void *args)
{
	SpawnedFunctionArgsBlock *argsBlock = (SpawnedFunctionArgsBlock *) args;
	assert(argsBlock != nullptr);

	// Call the user completion callback if present
	if (argsBlock->_completionCallback != nullptr) {
		argsBlock->_completionCallback(argsBlock->_completionArgs);
	}
}


//! Public API function to spawn functions
void nanos6_spawn_function(
	void (*function)(void *),
	void *args,
	void (*completion_callback)(void *),
	void *completion_args,
	char const *label
) {
	SpawnFunction::spawnFunction(
		function,
		args,
		completion_callback,
		completion_args,
		label,
		true
	);
}

//! Public API function to spawn functions in streams
void nanos6_stream_spawn_function(
	void (*function)(void *),
	void *args,
	void (*completion_callback)(void *),
	void *completion_args,
	char const *label,
	size_t stream_id
) {
	SpawnFunction::spawnFunction(
		function,
		args,
		completion_callback,
		completion_args,
		label,
		true,
		Task::nanos6_task_runtime_flag_t::nanos6_stream_flag,
		stream_id
	);
}
