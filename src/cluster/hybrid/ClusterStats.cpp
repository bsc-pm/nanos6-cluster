
/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include "ClusterStats.hpp"


std::unordered_map<void *, ClusterCPUStats *> ClusterStats::_CPUStats;
CPUStatsTimer ClusterStats::_intervalTimer;
SpinLock ClusterStats::_lock;
bool ClusterStats::_initialized = false;
busy_cores_t ClusterStats::_totalBusyShutdown = 0;
busy_cores_t ClusterStats::_usefulBusyShutdown = 0;
float ClusterStats::_elapsed = 0;
std::vector<ClusterAveragedStats*> ClusterStats::_listeners;

ClusterAveragedStats::ClusterAveragedStats(double timePeriodSecs, bool onStats, bool totalBusy)
			: _timePeriodSecs(timePeriodSecs),
			_movingAverage(0),
			_onStats(onStats),
			_time(0),
			_totalBusy(totalBusy)
	{
		if (onStats) {
			ClusterStats::pushBackListener(this);
		} else {
			_timer.startTimer();
		}
	}

ClusterAveragedStats::~ClusterAveragedStats()
{
	if (_onStats) {
		ClusterStats::removeListener(this);
	}
}
