/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_SCHEDULER_INTERFACE_HPP
#define CLUSTER_SCHEDULER_INTERFACE_HPP

#include "scheduling/Scheduler.hpp"
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

		virtual int getScheduledNode(
			Task *task,
			ComputePlace *computePlace,
			ReadyTaskHint hint = NO_HINT) = 0;

		// Try to steal a task for the local scheduler:
		// currently only for the cluster balance scheduler
		virtual Task *stealTask(ComputePlace *)
		{
			return nullptr;
		}

		virtual void offloadedTaskFinished(ClusterNode *)
		{
		}

		virtual void decNumLocalReadyTasks()
		{
		}

		virtual void incNumLocalReadyTasks()
		{
		}

		virtual int getNumLocalReadyTasks()
		{
			assert(false);
			return -1;
		}
	};

	typedef std::map<nanos6_cluster_scheduler_t, ClusterSchedulerPolicy*> scheduler_map_t;
protected:

	//! Current cluster node
	ClusterNode * const _thisNode;
	ClusterNode *_lastScheduledNode;

	scheduler_map_t _schedulerMap;

	ClusterSchedulerPolicy * const _defaultScheduler;
public:

	template<typename T>
	static bool RegisterClusterSchedulerPolicy(nanos6_cluster_scheduler_t id)
	{
		static_assert(std::is_base_of<ClusterSchedulerPolicy, T>::value, "Base class is wrong.");

		return GenericFactory<
			nanos6_cluster_scheduler_t,
			ClusterSchedulerPolicy *,
			ClusterSchedulerInterface *>::getInstance().emplace(
				id,
				[](ClusterSchedulerInterface *interface) -> ClusterSchedulerPolicy* {
					return new T(interface);
				}
			);
	}


	ClusterSchedulerPolicy *getOrCreateScheduler(nanos6_cluster_scheduler_t id)
	{
		scheduler_map_t::iterator it = _schedulerMap.lower_bound(id);

		if (it != _schedulerMap.end() && it->first == id) {
			return it->second;
		}

		ClusterSchedulerPolicy *ret = GenericFactory<
			nanos6_cluster_scheduler_t,
			ClusterSchedulerPolicy *,
			ClusterSchedulerInterface *>::getInstance().create(id, this);

		_schedulerMap[id] = ret;

		return ret;
	}

	void addReadyLocalOrExecuteRemote(
		int nodeId,
		Task *task,
		ComputePlace *computePlace,
		ReadyTaskHint hint);

	//! Handle constrains for cluster. Must return true only if some constrains where found and used
	//! properly.
	int handleClusterSchedulerConstrains(Task *task, ComputePlace *computePlace, ReadyTaskHint hint)
	{
		assert(!task->isTaskforCollaborator());
		//! We do not offload spawned functions, if0 tasks, remote task
		//! and tasks that already have an ExecutionWorkflow created for
		//! them
		if (task->isSpawned()             // Don't offload spawned tasks.
			|| task->isRemoteWrapper()    // This will save the day when we want offload spawned tasks
			|| task->isRemoteTask()       // Already offloaded don't re-offload
			|| task->isIf0()
			|| task->isPolling()          // Polling tasks
			|| (task->isTaskloop() && !task->isTaskloopSource()) // Taskfor sources can be offloaded, but not collaborators
			|| (task->getNode() == nanos6_cluster_no_offload)
			|| task->getWorkflow() != nullptr) {

			return nanos6_cluster_no_offload;
		}

		const int nodeId = task->getNode();

		if (nodeId >= 0) {                            // Id is a node number.
			FatalErrorHandler::failIf(
				nodeId >= ClusterManager::clusterSize(),
				"node in node() clause is out of range (",
				nodeId, " >= ", ClusterManager::clusterSize(),
				") in task: ", task->getLabel()
			);

			return nodeId;
		}

		const nanos6_cluster_scheduler_t schedulerId
			= static_cast<nanos6_cluster_scheduler_t>(nodeId);

		if (schedulerId == nanos6_cluster_no_hint) {  // Explicitly not hint set.
			return nanos6_cluster_no_hint;
		}

		// The cluster() value is a scheduler hint.
		if (schedulerId > nanos6_cluster_min_hint
			&& schedulerId < nanos6_cluster_no_offload) {

			ClusterSchedulerPolicy * policy = getOrCreateScheduler(schedulerId);
			assert(policy != nullptr);
			return policy->getScheduledNode(task, computePlace, hint);
		}

		FatalErrorHandler::fail(
			"hint value in node() constraint is out of range. nodeId:",
			nodeId, " in task: ", task->getLabel()
		);

		return nanos6_cluster_no_hint; // Avoid compiler warning
	}

	ClusterSchedulerInterface(nanos6_cluster_scheduler_t it);

	virtual ~ClusterSchedulerInterface()
	{
		for (auto it : _schedulerMap) {
			delete it.second;
			it.second = nullptr;
		}
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
			clusterhint = _defaultScheduler->getScheduledNode(task, computePlace, hint);
		}

		if (clusterhint != nanos6_cluster_no_schedule) {
			addReadyLocalOrExecuteRemote(clusterhint, task, computePlace, hint);
		}
	};

	Task *stealTask()
	{
		return nullptr;
	}

	virtual void offloadedTaskFinished(ClusterNode *remoteNode)
	{
		_defaultScheduler->offloadedTaskFinished(remoteNode);
	}

	virtual inline Task *getReadyTask(ComputePlace *computePlace)
	{
		// Get ready task: first try local scheduler
		Task *readyTask = SchedulerInterface::getReadyTask(computePlace);

		// If successful, decrease number of local ready tasks. Only useful
		// with the cluster balance scheduler cluster balance scheduler. Note:
		// taskfors are not counted at all, as we will need to think more
		// carefully how to do the accounting: currently the count would be
		// increased once per source and decremented once per collaborator.
		// This may cause the count to go negative, which is an assertion
		// failure.
		if (readyTask) {
			if (!readyTask->isTaskforSource() && !readyTask->isTaskforCollaborator()) {
				_defaultScheduler->decNumLocalReadyTasks();
			}
		}

		// If not successful, steal a task from the cluster scheduler
		// Only useful with the cluster balance scheduler
		if (!readyTask) {
			readyTask = _defaultScheduler->stealTask(computePlace);
		}
		return readyTask;
	}

};

#endif // CLUSTER_SCHEDULER_INTERFACE_HPP
