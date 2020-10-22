/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_HYBRID_MANAGER_HPP
#define CLUSTER_HYBRID_MANAGER_HPP

#include <cassert>
#include <list>
#include <string.h>
#include "DataAccessRegion.hpp"
#include <nanos6/cluster.h>
#include <sched.h>
#include "cluster/hybrid/ClusterStats.hpp"

enum class ClusterHybridPolicy {
	Bad = 0,
	Global,
	Local
};

class ClusterHybridManager {

private:
	static bool _inHybridClusterMode;
	static int _numCPUs;
	static int _numOwnedCPUs;
	static ClusterHybridPolicy _policy;
	static bool _dromEnabled;
	static int _allocOtherInstances;
	static float _busyOtherInstances;
	static float _averagedBusyOtherInstances;
	static float _averagedBusy;
	static ClusterAveragedStats *_averagedStatsBusyOtherInstances;

public:

	static void initialize(bool forceHybrid, size_t clusterSize);

	static bool inHybridClusterMode()
	{
		return _inHybridClusterMode;
	}

	static void getInitialCPUMask(cpu_set_t *set);

	static int countEnabledCPUs(void);

	static int getOwnedCPUs(void)
	{
		return _numOwnedCPUs;
	}

	static ClusterHybridPolicy getHybridPolicy(void)
	{
		assert(_policy != ClusterHybridPolicy::Bad);
		return _policy;
	}

	static void setEnableDROM(bool enabled)
	{
		_dromEnabled = enabled;
	}

	static bool getEnableDROM()
	{
		return _dromEnabled;
	}

	static void setDemandOtherInstances(int allocOtherInstances, float busyOtherInstances)
	{
		_allocOtherInstances = allocOtherInstances;
		_busyOtherInstances = busyOtherInstances;
		_averagedStatsBusyOtherInstances->update(busyOtherInstances);
	}

	static void setAveragedBusy(float averagedBusy)
	{
		_averagedBusy = averagedBusy;
	}

	static float getAveragedBusyOtherInstances()
	{
		return  _averagedStatsBusyOtherInstances->readBusyCores();
	}

	static float getAveragedBusy()
	{
		return  _averagedBusy;
	}

	static int getAllocOtherInstances()
	{
		return _allocOtherInstances;
	}

	static float getBusyOtherInstances()
	{
		return _busyOtherInstances;
	}

	static int getCurrentOwnedCPUs();
};

#endif /* CLUSTER_HYBRID_MANAGER_HPP */
