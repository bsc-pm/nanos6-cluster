/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTERHYBRIDMETRICS_HPP
#define CLUSTERHYBRIDMETRICS_HPP

#include <atomic>
#include <vector>
#include "lowlevel/SpinLock.hpp"
#include <mutex>

#include "InstrumentCluster.hpp"

class ClusterHybridMetrics {
private:
	static SpinLock _promisedTasksLock;
	static std::atomic<size_t> _numReadyTasks;
	static std::atomic<size_t> _numImmovableReadyTasks;

public:

	static inline void incNumReadyTasks(int by)
	{
		_numReadyTasks += by;
	}

	static inline void incNumImmovableReadyTasks(int by)
	{
	        _numImmovableReadyTasks += by;
			Instrument::emitClusterEvent(Instrument::ClusterEventType::ImmovableTasks, _numImmovableReadyTasks);
	}
	
	static inline size_t getNumImmovableReadyTasks()
	{
	        return _numImmovableReadyTasks;
	}

	static inline size_t getNumReadyTasks()
	{
		return _numReadyTasks + _numImmovableReadyTasks;
	}

};

#endif // CLUSTERHYBRIDMETRICS_HPP
