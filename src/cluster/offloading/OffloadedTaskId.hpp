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


class OffloadedTaskIdManager
{
public:
	typedef size_t OffloadedTaskId;
	constexpr static OffloadedTaskId InvalidOffloadedTaskId = 0;

private:
	static constexpr int logMaxNodes = 8;
	std::atomic<size_t> _counter;
	static OffloadedTaskIdManager *_singleton;

	OffloadedTaskIdManager() : _counter(0)
	{
	}

public:
	static void initialize(int nodeIndex, int clusterSize)
	{
		assert(_singleton == nullptr);
		_singleton = new OffloadedTaskIdManager();
		OffloadedTaskIdManager::reset(nodeIndex, clusterSize);
	}

	static void reset(int nodeIndex, __attribute__((unused)) int clusterSize)
	{
		assert(_singleton != nullptr);
		assert(clusterSize < (1 << logMaxNodes));
		_singleton->_counter = 1 + (((size_t)nodeIndex) << (64 - logMaxNodes));
	}

	static OffloadedTaskIdManager::OffloadedTaskId nextOffloadedTaskId()
	{
		assert(_singleton != nullptr);
		return _singleton->_counter.fetch_add(1);
	}

	static void finalize()
	{
		assert(_singleton != nullptr);
		delete _singleton;
		_singleton = nullptr;
	}
};

#endif /* OFFLOADED_TASK_ID_H */
