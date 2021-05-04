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
	static std::atomic<size_t> _directOffload;
	static std::atomic<size_t> _directThiefOffload;
	static std::atomic<size_t> _sendMoreOffload;
	static std::atomic<size_t> _checkOffload;
	static std::atomic<size_t> _sentNumNewTask;
	static std::atomic<size_t> _receivedNumNewTask;
	static std::atomic<size_t> _sentNumTaskFinished;
	static std::atomic<size_t> _receivedNumTaskFinished;

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

	static inline void incDirectOffload(int by)
	{
		_directOffload += by;
	}

	static inline size_t getDirectOffload()
	{
		return _directOffload;
	}

	static inline void incDirectThiefOffload(int by)
	{
		_directThiefOffload += by;
	}

	static inline size_t getDirectThiefOffload()
	{
		return _directThiefOffload;
	}

	static inline void incSendMoreOffload(int by)
	{
		_sendMoreOffload += by;
	}

	static inline size_t getSendMoreOffload()
	{
		return _sendMoreOffload;
	}

	static inline void incCheckOffload(int by)
	{
		_checkOffload += by;
	}

	static inline size_t getCheckOffload()
	{
		return _checkOffload;
	}

	static inline void incSentNumNewTask()
	{
		_sentNumNewTask ++;
	}
	static inline size_t getSentNumNewTask()
	{
		return _sentNumNewTask;
	}

	static inline void incReceivedNumNewTask()
	{
		_receivedNumNewTask ++;
	}
	static inline size_t getReceivedNumNewTask()
	{
		return _receivedNumNewTask;
	}

	static inline void incSentNumTaskFinished()
	{
		_sentNumTaskFinished ++;
	}
	static inline size_t getSentNumTaskFinished()
	{
		return _sentNumTaskFinished;
	}

	static inline void incReceivedNumTaskFinished()
	{
		_receivedNumTaskFinished ++;
	}
	static inline size_t getReceivedNumTaskFinished()
	{
		return _receivedNumTaskFinished;
	}
};

#endif // CLUSTERHYBRIDMETRICS_HPP
