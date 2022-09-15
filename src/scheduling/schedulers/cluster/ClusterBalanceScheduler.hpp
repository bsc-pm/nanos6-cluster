/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_BALANCE_SCHEDULER_HPP
#define CLUSTER_BALANCE_SCHEDULER_HPP

#include <atomic>
#include <list>
#include "scheduling/schedulers/cluster/ClusterSchedulerInterface.hpp"
#include "system/RuntimeInfo.hpp"
#include "lowlevel/SpinLock.hpp"

#include <ClusterManager.hpp>

class StealableTask
{
	public:
		Task *_task;
		std::vector<float> _affinityFrac;

		StealableTask(Task *task, std::vector<size_t> &affinityBytes)
			: _task(task)
		{
			_affinityFrac.resize(affinityBytes.size());
			int tot=0;
			for(int bytes : affinityBytes) {
				tot += bytes;
			}
			for (size_t i=0; i < affinityBytes.size(); i++) {
				_affinityFrac[i] = 1.0 * affinityBytes[i] / tot;
			}
		}
};

class ClusterBalanceScheduler : public ClusterSchedulerInterface::ClusterSchedulerPolicy {
private:
	std::vector<std::list<StealableTask*> >  _readyQueues;
	std::atomic<int> _numLocalReadyTasks;
	SpinLock _readyQueueLock;
	// SpinLock _requestLock;

	inline size_t getNodeIdForLocation(MemoryPlace const *location) const
	{
		if (location->getType() == nanos6_host_device) {
			return _interface->getThisNode()->getIndex();
		}

		return location->getIndex();
	}

	Task *stealTask(ClusterNode *targetNode);

public:
	ClusterBalanceScheduler(ClusterSchedulerInterface * const interface)
		: ClusterSchedulerPolicy("ClusterBalanceScheduler", interface),
		_numLocalReadyTasks(0)
	{
		for (int i=0; i < ClusterManager::clusterSize(); i++) {
			_readyQueues.emplace_back(0);
		}
	}

	~ClusterBalanceScheduler()
	{
	}

	int getScheduledNode(
		Task *task,
		ComputePlace *computePlace,
		ReadyTaskHint hint
	) override;

	Task *stealTask(ComputePlace *computePlace) override;

	void offloadedTaskFinished(ClusterNode *remoteNode) override;

	void checkSendMoreAllNodes();

	void decNumLocalReadyTasks() override
	{
		_numLocalReadyTasks--;
		assert(_numLocalReadyTasks >= 0);
	}

	void incNumLocalReadyTasks() override
	{
		_numLocalReadyTasks++;
	}

	int getNumLocalReadyTasks() override
	{
		return _numLocalReadyTasks;
	}
};

static const bool __attribute__((unused))_registered_balance_sched =
	ClusterSchedulerInterface::RegisterClusterSchedulerPolicy<ClusterBalanceScheduler>(nanos6_cluster_balance);

#endif // CLUSTER_BALANCE_SCHEDULER_HPP
