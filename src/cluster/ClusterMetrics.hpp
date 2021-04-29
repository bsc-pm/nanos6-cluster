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

	// Debug metrics for ClusterBalanceScheduler
	static std::atomic<size_t> _directSelf;
	static std::atomic<size_t> _directOffload;
	static std::atomic<size_t> _directThiefOffload;
	static std::atomic<size_t> _directThiefSelf;
	static std::atomic<size_t> _stealSelf;
	static std::atomic<size_t> _sendMoreOffload;
	static std::atomic<size_t> _checkOffload;

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

	//! --------------------- Debug metrics for ClusterBalanceScheduler --------------------
	//! \brief Increment the number of tasks sent immediately to the host scheduler
	static inline void incDirectSelf(int by)
	{
		_directSelf += by;
	}

	//! \brief Get the cumulative number of tasks sent immediately to the host scheduler
	static inline size_t getDirectSelf()
	{
		return _directSelf;
	}

	//! \brief Increment the number of tasks immediately offloaded to another node
	static inline void incDirectOffload(int by)
	{
		_directOffload += by;
	}

	//! \brief Get the cumulative number of tasks immediately offloadde to another node
	static inline size_t getDirectOffload()
	{
		return _directOffload;
	}

	//! \brief Increment the number of tasks immediately stolen by the host scheduler
	static inline void incDirectThiefSelf(int by)
	{
		_directThiefSelf += by;
	}

	//! \brief Get the cumulative number of tasks immediately stolen by the host scheduler
	//!
	//! These are tasks that would have been scheduled to another node, but the other node
	//! was busy and the task was immediately stolen by the local host scheduler.
	static inline size_t getDirectThiefSelf()
	{
		return _directThiefSelf;
	}

	//! \brief Increment the number of tasks immediately stolen by another node
	static inline void incDirectThiefOffload(int by)
	{
		_directThiefOffload += by;
	}

	//! \brief Get the cumulative number of tasks immediately stolen by another node
	//!
	//! These are tasks that would have been scheduled to a different node (perhaps the
	//! local node), but that node was busy and the task was immediately stolen by
	//! another node
	static inline size_t getDirectThiefOffload()
	{
		return _directThiefOffload;
	}

	//! \brief Increment the number of tasks stolen by the host scheduler from another node
	static inline void incStealSelf(int by)
	{
		_stealSelf += by;
	}

	//! \brief Get the cumulative number of tasks stolen by the host scheduler from another node
	//!
	//! These tasks weren't stolen immediately, but later when the host scheduler needed a ready task.
	static inline size_t getStealSelf()
	{
		return _stealSelf;
	}

	//! \brief Increment the number of tasks scheduled via checkSendMoreAllNodes
	static inline void incSendMoreOffload(int by)
	{
		_sendMoreOffload += by;
	}

	//! \brief Get the cumulative number of tasks scheduled via checkSendMoreAllNodes
	static inline size_t getSendMoreOffload()
	{
		return _sendMoreOffload;
	}

	//! \brief Increment the number of tasks scheduled when an offloaded task finishes
	static inline void incCheckOffload(int by)
	{
		_checkOffload += by;
	}

	//! \brief Get the cumulative number of tasks scheduled when an offloaded task finishes
	static inline size_t getCheckOffload()
	{
		return _checkOffload;
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
