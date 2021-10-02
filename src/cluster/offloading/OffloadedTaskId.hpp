/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef OFFLOADED_TASK_ID_H
#define OFFLOADED_TASK_ID_H

#include <cassert>
#include <cstddef>
#include <atomic>

class Task;

typedef size_t OffloadedTaskId;
constexpr OffloadedTaskId InvalidOffloadedTaskId = 0;

class OffloadedTaskIdManager
{
private:
	static constexpr int logMaxNodes = 8;
	static std::atomic<size_t> _counter;

public:
	static void initialize(int nodeIndex, __attribute__((unused)) int clusterSize)
	{
		assert(clusterSize < (1 << logMaxNodes));
		_counter = 1 + (((size_t)nodeIndex) << (64 - logMaxNodes));
	}

	static OffloadedTaskId nextOffloadedTaskId()
	{
		return _counter.fetch_add(1);
	}
};

// inline Task *getOriginalTask(OffloadedTaskId taskId)
// {
// 	return reinterpret_cast<Task *>(taskId);
// }
// 
// inline OffloadedTaskId getTaskId(Task *task)
// {
// 	return reinterpret_cast<OffloadedTaskId>(task);
// }

#endif /* OFFLOADED_TASK_ID_H */
