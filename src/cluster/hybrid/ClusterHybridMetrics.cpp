/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2021 Barcelona Supercomputing Center (BSC)
*/

#include "cluster/hybrid/ClusterHybridMetrics.hpp"

SpinLock ClusterHybridMetrics::_promisedTasksLock;
std::atomic<size_t> ClusterHybridMetrics::_numReadyTasks(0);
std::atomic<size_t> ClusterHybridMetrics::_numImmovableReadyTasks(0);

