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

		virtual int addReadyTask(
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
		int nodeId,
		Task *task,
		ComputePlace *computePlace,
		ReadyTaskHint hint);

	//! Handle constrains for cluster. Must return true only if some constrains where found and used
	//! properly.
	int handleClusterSchedulerConstrains(Task *task, ComputePlace *, ReadyTaskHint)
	{
		//! We do not offload spawned functions, if0 tasks, remote task
		//! and tasks that already have an ExecutionWorkflow created for
		//! them
		if (task->isSpawned()             // Don't offload spawned tasks.
			|| task->isRemoteWrapper()    // This will save the day when we want offload spawned tasks
			|| task->isRemoteTask()       // Already offloaded don't re-offload
			|| task->isIf0()
			|| task->isPolling()          // Polling tasks
			|| task->isTaskloop()         // for now don't offload task{loop,for}
			|| task->isTaskfor()
			|| task->getWorkflow() != nullptr) {

			return nanos6_cluster_no_offload;
		}

		if (task->hasConstrains()) {
			const int nodeId = task->getNode();
			FatalErrorHandler::failIf(
				nodeId >= ClusterManager::clusterSize(),
				"node in node() constraint out of range"
			);

			if (nodeId != nanos6_cluster_no_offload) {
				return nodeId;
			}
		}

		return nanos6_cluster_no_hint;
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

		int clusterhint = handleClusterSchedulerConstrains(task, computePlace, hint);

		if (clusterhint == nanos6_cluster_no_hint) {
			clusterhint = _defaultScheduler->addReadyTask(task, computePlace, hint);
		}

		addReadyLocalOrExecuteRemote(clusterhint, task, computePlace, hint);

	};

};

#endif // CLUSTER_SCHEDULER_INTERFACE_HPP
