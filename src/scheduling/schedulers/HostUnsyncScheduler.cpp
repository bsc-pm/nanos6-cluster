/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "HostUnsyncScheduler.hpp"
#include "scheduling/ready-queues/DeadlineQueue.hpp"
#include "scheduling/ready-queues/ReadyQueueDeque.hpp"
#include "scheduling/ready-queues/ReadyQueueMap.hpp"
#include "tasks/LoopGenerator.hpp"
#include "tasks/Task.hpp"
#include "tasks/Taskfor.hpp"

Task *HostUnsyncScheduler::getReadyTask(ComputePlace *computePlace)
{
	assert(computePlace != nullptr);
	assert(_deadlineTasks != nullptr);
	assert(_readyTasks != nullptr);

	Task *result = nullptr;

	const long cpuId = computePlace->getIndex();
	const long groupId = ((CPU *)computePlace)->getGroupId();
	const long immediateSuccessorGroupId = groupId * 2;

	// 1. Try to get a task with a satisfied deadline
	result = _deadlineTasks->getReadyTask(computePlace);
	if (result != nullptr) {
		return result;
	}

	Task::priority_t topPriority;

retry:
	// 2a. Try to get work from the current group taskfor
	if (groupId != -1) {
		Taskfor *groupTaskfor = _groupSlots[groupId];

		// Get the priority of the highest priority ready task
		if (groupTaskfor || !_interruptedTaskfors.empty()) {
			topPriority = _readyTasks->getNextTaskPriority();
		}

		if (groupTaskfor != nullptr) {
			long priority = groupTaskfor->getPriority();
			if (priority >= topPriority) {
				groupTaskfor->notifyCollaboratorHasStarted();
				bool remove = false;
				int myChunk = groupTaskfor->getNextChunk(cpuId, &remove);
				if (remove) {
					_groupSlots[groupId] = nullptr;
					groupTaskfor->removedFromScheduler();
				}

				Taskfor *taskfor = computePlace->getPreallocatedTaskfor();
				// We are setting the chunk that the collaborator will execute in the preallocatedTaskfor
				taskfor->setChunk(myChunk);
				return groupTaskfor;
			} else {
				// Interrupt this taskfor for a higher priority task (may itself be
				// a taskfor). Push the current taskfor onto the interrupted taskfor
				// list, so it can be resumed later.
				_interruptedTaskfors.push_back(groupTaskfor);
				_groupSlots[groupId] = nullptr;
			}
		}

		// 2b. Try to continue an interrupted taskfor if it is same or higher priority
		// than the current highest priority task
		if (!_interruptedTaskfors.empty()) {
			auto itBest = _interruptedTaskfors.end();
			Task::priority_t best = topPriority;
			for (auto it = _interruptedTaskfors.begin(); it != _interruptedTaskfors.end(); it++) {
				Taskfor *taskfor = *it;
				if (taskfor->getPriority() >= best) {
					itBest = it;
					best = taskfor->getPriority();
				}
			}
			if (itBest != _interruptedTaskfors.end()) {
				// Resume the interrupted taskfor
				_groupSlots[groupId] = *itBest;
				_interruptedTaskfors.erase(itBest);
				goto retry;
			}
		}
	}

	if (_enableImmediateSuccessor) {
		// 3. Try to get work from my immediateSuccessorTaskfors
		Task *currentImmediateSuccessor1 = _immediateSuccessorTaskfors[immediateSuccessorGroupId];
		Task *currentImmediateSuccessor2 = _immediateSuccessorTaskfors[immediateSuccessorGroupId+1];
		if (currentImmediateSuccessor1 != nullptr) {
			assert(currentImmediateSuccessor1->isTaskfor());
			result = currentImmediateSuccessor1;
			_immediateSuccessorTaskfors[immediateSuccessorGroupId] = nullptr;
		}
		else if (currentImmediateSuccessor2 != nullptr) {
			assert(currentImmediateSuccessor2->isTaskfor());
			result = currentImmediateSuccessor2;
			_immediateSuccessorTaskfors[immediateSuccessorGroupId+1] = nullptr;
		}

		// 4. Try to get work from my immediateSuccessorTasks
		if (result == nullptr && _immediateSuccessorTasks[cpuId] != nullptr) {
			result = _immediateSuccessorTasks[cpuId];
			_immediateSuccessorTasks[cpuId] = nullptr;
		}
	}

	// 5. Check if there is work remaining in the ready queue
	if (result == nullptr) {
		result = _readyTasks->getReadyTask(computePlace);
	}

	// 6. Try to get work from other immediateSuccessorTasks
	if (result == nullptr && _enableImmediateSuccessor) {
		for (size_t i = 0; i < _immediateSuccessorTasks.size(); i++) {
			if (_immediateSuccessorTasks[i] != nullptr) {
				result = _immediateSuccessorTasks[i];
				assert(!result->isTaskfor());
				_immediateSuccessorTasks[i] = nullptr;
				break;
			}
		}
	}

	// 7. Try to get work from other immediateSuccessorTasksfors
	if (result == nullptr && _enableImmediateSuccessor) {
		for (size_t i = 0; i < _immediateSuccessorTaskfors.size(); i++) {
			if (_immediateSuccessorTaskfors[i] != nullptr) {
				result = _immediateSuccessorTaskfors[i];
				_immediateSuccessorTaskfors[i] = nullptr;
				break;
			}
		}
	}

	if (result == nullptr
		|| !result->isTaskforSource()
		|| (result->isTaskforSource() && result->getWorkflow() == nullptr)) {
		return result;
	}

	assert(result->isTaskfor());
	assert(computePlace->getType() == nanos6_host_device);

	Taskfor *taskfor = (Taskfor *) result;
	_groupSlots[groupId] = taskfor;
	taskfor->markAsScheduled();
	return getReadyTask(computePlace);
}
