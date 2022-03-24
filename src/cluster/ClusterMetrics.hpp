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

class ClusterMetrics {
private:
	static std::atomic<size_t> _numReadyTasks;
	static std::atomic<size_t> _numImmovableTasks;
	static std::atomic<size_t> _totalBusyCoresCurrentApprank;

public:

	static inline void incNumReadyTasks(int by)
	{
		_numReadyTasks += by;
	}

	static inline void incNumImmovableTasks(int by)
	{
		_numImmovableTasks += by;
		Instrument::emitClusterEvent(Instrument::ClusterEventType::ImmovableTasks, _numImmovableTasks);
	}

	static inline size_t getNumImmovableTasks()
	{
		return _numImmovableTasks;
	}

	static inline size_t getNumReadyTasks()
	{
		return _numReadyTasks + _numImmovableTasks;
	}

	//! \brief Set the total number of busy cores across this whole apprank
	static void setTotalBusyCoresCurrentApprank(int totalBusyCoresCurrentApprank)
	{
		_totalBusyCoresCurrentApprank = totalBusyCoresCurrentApprank;
	}

	//! \brief Get the total number of busy cores across this whole apprank
	static int getTotalBusyCoresCurrentApprank()
	{
		return _totalBusyCoresCurrentApprank;
	}
};

#endif // CLUSTERHYBRIDMETRICS_HPP
