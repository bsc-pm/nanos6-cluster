/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "UnsyncScheduler.hpp"
#include "dependencies/DataTrackingSupport.hpp"
#include "executors/threads/CPUManager.hpp"


UnsyncScheduler::UnsyncScheduler(
	SchedulingPolicy,
	bool enablePriority,
	bool enableImmediateSuccessor
) :
	_queues(nullptr),
	_numQueues(0),
	_roundRobinQueues(0),
	_deadlineTasks(nullptr),
	_enableImmediateSuccessor(enableImmediateSuccessor),
	_enablePriority(enablePriority)
{
	if (enableImmediateSuccessor) {
		_immediateSuccessorTasks = immediate_successor_tasks_t(CPUManager::getTotalCPUs(), nullptr);
	}
}

UnsyncScheduler::~UnsyncScheduler()
{
	assert(_numQueues > 0);

	for (uint64_t i = 0; i < _numQueues; i++) {
		if (_queues[i] != nullptr) {
			delete _queues[i];
		}
	}

	MemoryAllocator::free(_queues, _numQueues * sizeof(ReadyQueue *));
}

void UnsyncScheduler::regularAddReadyTask(Task *task, bool unblocked)
{
	uint64_t NUMAid = task->getNUMAHint();

	// In case there is no hint, use round robin to balance the load
	if (NUMAid == (uint64_t) -1) {
		do {
			NUMAid = _roundRobinQueues;
			_roundRobinQueues = (_roundRobinQueues + 1) % _numQueues;
		} while (_queues[NUMAid] == nullptr);
	}

	assert(NUMAid < _numQueues);

	assert(_queues[NUMAid] != nullptr);
	_queues[NUMAid]->addReadyTask(task, unblocked);
}

Task *UnsyncScheduler::regularGetReadyTask(ComputePlace *computePlace)
{
	uint64_t NUMAid = 0;

	if (_numQueues > 1) {
		assert(computePlace->getType() == nanos6_host_device);
		NUMAid = ((CPU *)computePlace)->getNumaNodeId();
	}
	assert(NUMAid < _numQueues);

	Task *result = nullptr;
	result = _queues[NUMAid]->getReadyTask(computePlace);
	if (result != nullptr)
		return result;

	if (_numQueues > 1) {
		// Try to steal considering distance and load balance
		const std::vector<uint64_t> &distances = HardwareInfo::getNUMADistances();

		const double distanceThold = DataTrackingSupport::getDistanceThreshold();
		const double loadThold = DataTrackingSupport::getLoadThreshold();

		// Ideally, we want to steal from closer sockets with many tasks, so we
		// will use this score function: score = 100/distance + ready_tasks/5
		// The highest score the better
		uint64_t score = 0;
		uint64_t chosen = (uint64_t) -1;
		for (uint64_t q = 0; q < _numQueues; q++) {
			if (q != NUMAid && _queues[q] != nullptr) {
				size_t numReadyTasks = _queues[q]->getNumReadyTasks();

				if (numReadyTasks > 0) {
					uint64_t distance = distances[q * _numQueues + NUMAid];
					uint64_t loadFactor = numReadyTasks;

					if (distance < distanceThold && loadFactor > loadThold) {
						chosen = q;
						break;
					}

					assert(distance != 0);
					uint64_t tmpscore = 100 / distance + loadFactor / 5;
					if (tmpscore >= score) {
						score = tmpscore;
						chosen = q;
					}
				}
			}
		}

		if (chosen != (uint64_t) -1) {
			auto m = _reservedToSteal.find(computePlace);

			// Which task did we reserve (countdown may not be zero yet)
			Task *reserved = (m != _reservedToSteal.end()) ? m->second.task : nullptr;

			// If the countdown reached zero, then the reserved task is ready to be stolen
			Task *ready    = (m != _reservedToSteal.end() && m->second.count == 0) ? m->second.task : nullptr;

			// Steal the task if it is the one ready to be stolen, otherwise find out which task we should reserve
			Task *toSteal = _queues[chosen]->tryReadyTask(computePlace, ready);
			assert(toSteal != nullptr);

			if (toSteal == ready) {
				// We stole the ready-to-steal reserved task
				result = toSteal;
			} else if (toSteal == reserved) {
				// The task was reserved, but countdown isn't yet zero: decrement the countdown
				// We want to give cores on the right NUMA node an opportunity to take it before we steal it
				m->second.count --;
			} else {
				// A new (or different) task to steal: keep a record to reserve it at this compute place
				_reservedToSteal[computePlace] = ReservedTask{toSteal, 10};
			}
		}
	}

	return result;
}
