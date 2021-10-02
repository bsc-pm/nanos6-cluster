/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef OFFLOADED_TASK_ID_H
#define OFFLOADED_TASK_ID_H

#include <cassert>
#include <cstddef>

class Task;

typedef size_t OffloadedTaskId;
constexpr OffloadedTaskId InvalidOffloadedTaskId = 0;

inline Task *getOriginalTask(OffloadedTaskId taskId)
{
	return reinterpret_cast<Task *>(taskId);
}

inline OffloadedTaskId getTaskId(Task *task)
{
	return reinterpret_cast<OffloadedTaskId>(task);
}

#endif /* OFFLOADED_TASK_ID_H */
