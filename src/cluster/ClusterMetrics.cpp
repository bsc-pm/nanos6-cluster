/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2021 Barcelona Supercomputing Center (BSC)
*/

#include "cluster/ClusterMetrics.hpp"

std::atomic<size_t> ClusterMetrics::_numReadyTasks(0);
std::atomic<size_t> ClusterMetrics::_numImmovableTasks(0);
std::atomic<size_t> ClusterMetrics::_totalBusyCoresCurrentApprank;

