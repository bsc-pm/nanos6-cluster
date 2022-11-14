/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_HYBRID_MANAGER_HPP
#define CLUSTER_HYBRID_MANAGER_HPP

#include "ClusterHybridInterface.hpp"

#include <cassert>
#include <list>
#include <string.h>
#include "DataAccessRegion.hpp"
#include <nanos6/cluster.h>
#include <sched.h>
#include "monitoring/RuntimeStateMonitor.hpp"

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
	static bool _hybridInterfaceFileInitialized;
	static int _allocOtherInstancesSameNode;
	static float _busyOtherInstancesSameNode;
	static float _averagedBusyOtherInstancesSameNode;
	static float _averagedBusy;
	static AveragedStats *_averagedStatsBusyOtherInstancesSameNode;

	//! Cluster hybrid interface for coordination among appranks
	static ClusterHybridInterface *_hyb;

public:

	static void preinitialize(
		bool forceHybrid,
		int externalRank,
		int apprankNum,
		int internalRank,
		int physicalNodeNum,
		int indexThisPhysicalNode,
		size_t clusterSize,
		const std::vector<int> &internalRankToExternalRank,
		const std::vector<int> &instanceThisNodeToExternalRank);

	static void initialize();

	static bool inHybridClusterMode()
	{
		return _inHybridClusterMode;
	}

	//! \brief In hybrid cluster mode, update numbers of cores per instance
	static void poll()
	{
		if(_hyb) {
			_hyb->poll();
		}
	}

	static void getInitialCPUMask(cpu_set_t *set);

	static int getOwnedCPUs(void)
	{
		return _numOwnedCPUs;
	}

	static ClusterHybridPolicy getHybridPolicy(void)
	{
		assert(_policy != ClusterHybridPolicy::Bad);
		return _policy;
	}

	static void setHybridInterfaceFileInitialized(bool enabled)
	{
		_hybridInterfaceFileInitialized = enabled;
	}

	static bool getHybridInterfaceFileInitialized()
	{
		return _hybridInterfaceFileInitialized;
	}

	static void setDemandOtherInstancesSameNode(int allocOtherInstancesSameNode, float busyOtherInstancesSameNode)
	{
		_allocOtherInstancesSameNode = allocOtherInstancesSameNode;
		_busyOtherInstancesSameNode = busyOtherInstancesSameNode;
		_averagedStatsBusyOtherInstancesSameNode->update(busyOtherInstancesSameNode);
	}

	static void setAveragedBusy(float averagedBusy)
	{
		_averagedBusy = averagedBusy;
	}

	static float getAveragedBusyOtherInstancesSameNode()
	{
		return  _averagedStatsBusyOtherInstancesSameNode->readBusyCores();
	}

	static float getAveragedBusy()
	{
		return  _averagedBusy;
	}

	static int getAllocOtherInstancesSameNode()
	{
		return _allocOtherInstancesSameNode;
	}

	static float getBusyOtherInstancesSameNode()
	{
		return _busyOtherInstancesSameNode;
	}

	//! Get the current number of owned CPUs
	static int getCurrentOwnedCPUs();

	static int getCurrentOwnedOrGivingCPUs();

	static int getCurrentLentOwnedCPUs();

	static int getCurrentBorrowedCPUs();

	static int getCurrentActiveOwnedCPUs();
};

#endif /* CLUSTER_HYBRID_MANAGER_HPP */
