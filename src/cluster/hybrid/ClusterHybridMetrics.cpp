/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2021 Barcelona Supercomputing Center (BSC)
*/

#include "cluster/hybrid/ClusterHybridMetrics.hpp"

SpinLock ClusterHybridMetrics::_promisedTasksLock;
std::atomic<size_t> ClusterHybridMetrics::_numReadyTasks(0);
std::atomic<size_t> ClusterHybridMetrics::_numImmovableTasks(0);
std::atomic<size_t> ClusterHybridMetrics::_directOffload;
std::atomic<size_t> ClusterHybridMetrics::_directSelf;
std::atomic<size_t> ClusterHybridMetrics::_directThiefOffload;
std::atomic<size_t> ClusterHybridMetrics::_directThiefSelf;
std::atomic<size_t> ClusterHybridMetrics::_sendMoreOffload;
std::atomic<size_t> ClusterHybridMetrics::_checkOffload;
std::atomic<size_t> ClusterHybridMetrics::_sentNumNewTask;
std::atomic<size_t> ClusterHybridMetrics::_receivedNumNewTask;
std::atomic<size_t> ClusterHybridMetrics::_sentNumTaskFinished;
std::atomic<size_t> ClusterHybridMetrics::_receivedNumTaskFinished;
std::atomic<size_t> ClusterHybridMetrics::_stealSelf;

