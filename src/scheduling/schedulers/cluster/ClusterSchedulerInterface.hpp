/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_SCHEDULER_INTERFACE_HPP
#define CLUSTER_SCHEDULER_INTERFACE_HPP

#include "scheduling/SchedulerInterface.hpp"
#include "system/RuntimeInfo.hpp"

#include <ClusterManager.hpp>

class ClusterSchedulerInterface : public SchedulerInterface {

public:
	class ClusterSchedulerPolicy {
	protected:
		const std::string _name;
		ClusterSchedulerInterface * const _interface;

	public:
		const std::string &getName() const { return _name; }

		ClusterSchedulerPolicy(const std::string &name, ClusterSchedulerInterface * const interface)
			: _name(name), _interface(interface)
		{
			assert(_interface != nullptr);
		}

		virtual ~ClusterSchedulerPolicy()
		{
		}

		virtual void addReadyTask(
			Task *task,
			ComputePlace *computePlace,
			ReadyTaskHint hint = NO_HINT) = 0;
	};

protected:

	//! Current cluster node
	ClusterNode * const _thisNode;
	ClusterNode *_lastScheduledNode;

	ClusterSchedulerPolicy *_defaultScheduler;

	//! Function to pass the task to the local scheduler or call the execute function in workflow
	//! when the task is remote.
public:

	void addReadyLocalOrExecuteRemote(
		size_t nodeId,
		Task *task,
		ComputePlace *computePlace,
		ReadyTaskHint hint);

	//! Handle constrains for cluster. Must return true only if some constrains where found and used
	//! properly.
	bool handleClusterSchedulerConstrains(Task *task, ComputePlace *computePlace, ReadyTaskHint hint);

	void addLocalReadyTask(Task *task, ComputePlace *computePlace, ReadyTaskHint hint = NO_HINT)
	{
		_lastScheduledNode = _thisNode;
		SchedulerInterface::addReadyTask(task, computePlace, hint);
	}

	ClusterSchedulerInterface();

	virtual ~ClusterSchedulerInterface()
	{
		delete _defaultScheduler;
	}

	inline std::string getName() const
	{
		return _defaultScheduler->getName();
	}

	inline ClusterNode *getThisNode() const
	{
		return _thisNode;
	}


	void addReadyTask(
		Task *task,
		ComputePlace *computePlace,
		ReadyTaskHint hint = NO_HINT
	) override {

		if (handleClusterSchedulerConstrains(task, computePlace, hint)) {
			return;
		}

		_defaultScheduler->addReadyTask(task, computePlace, hint);
	};

};

#endif // CLUSTER_SCHEDULER_INTERFACE_HPP
