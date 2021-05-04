/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2021 Barcelona Supercomputing Center (BSC)
*/

#include "cluster/ClusterMetrics.hpp"

std::atomic<size_t> ClusterMetrics::_numReadyTasks(0);
std::atomic<size_t> ClusterMetrics::_numImmovableTasks(0);
std::atomic<size_t> ClusterMetrics::_sentNumNewTask;
std::atomic<size_t> ClusterMetrics::_receivedNumNewTask;
std::atomic<size_t> ClusterMetrics::_sentNumTaskFinished;
std::atomic<size_t> ClusterMetrics::_receivedNumTaskFinished;
std::atomic<size_t> ClusterMetrics::_totalBusyCoresCurrentApprank;

// Debug metrics for ClusterBalanceScheduler
std::atomic<size_t> ClusterMetrics::_directSelf;
std::atomic<size_t> ClusterMetrics::_directOffload;
std::atomic<size_t> ClusterMetrics::_directThiefSelf;
std::atomic<size_t> ClusterMetrics::_directThiefOffload;
std::atomic<size_t> ClusterMetrics::_stealSelf;
std::atomic<size_t> ClusterMetrics::_sendMoreOffload;
std::atomic<size_t> ClusterMetrics::_checkOffload;
