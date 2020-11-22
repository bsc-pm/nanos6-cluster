/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include <cassert>

#include "ExecutionWorkflow.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "executors/threads/TaskFinalization.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "memory/directory/Directory.hpp"
#include "system/TrackingPoints.hpp"
#include "tasks/TaskImplementation.hpp"
#include "src/memory/directory/Directory.hpp"

#include <ClusterManager.hpp>
#include <DataAccess.hpp>
#include <DataAccessRegistration.hpp>
#include <DataAccessRegistrationImplementation.hpp>
#include <ExecutionWorkflowHost.hpp>
#include <ExecutionWorkflowCluster.hpp>
#include <ClusterManager.hpp>


namespace ExecutionWorkflow {

	transfers_map_t _transfersMap = {{
		/*              host         cuda      opencl    cluster   */
		/* host */    { nullCopy,    nullCopy, nullCopy, clusterCopy },
		/* cuda */    { nullCopy,    nullCopy, nullCopy, nullCopy },
		/* opencl */  { nullCopy,    nullCopy, nullCopy, nullCopy },
		/* cluster */ { clusterCopy, nullCopy, nullCopy, clusterCopy }
		}};

	Step *WorkflowBase::createDataCopyStep(
		MemoryPlace const *sourceMemoryPlace,
		MemoryPlace const *targetMemoryPlace,
		DataAccessRegion const &region,
		DataAccess *access
	) {
		/* At the moment we do not support data copies for accesses
			* of the following types. This essentially mean that devices,
			* e.g. Cluster, CUDA, do not support these accesses. */
		if (access->getType() == REDUCTION_ACCESS_TYPE ||
			access->getType() == COMMUTATIVE_ACCESS_TYPE ||
			access->getType() == CONCURRENT_ACCESS_TYPE
		) {
			return new Step();
		}

		assert(targetMemoryPlace != nullptr);
		//assert(sourceMemoryPlace != nullptr);

		const nanos6_device_t sourceType =
			(sourceMemoryPlace == nullptr) ? nanos6_host_device : sourceMemoryPlace->getType();

		const nanos6_device_t targetType = targetMemoryPlace->getType();

		/* Starting workflow for a task on the host: not in a namespace */
		if (targetType == nanos6_host_device ||
			targetMemoryPlace == ClusterManager::getCurrentMemoryNode()) {
				access->setValidNamespace( ClusterManager::getCurrentMemoryNode()->getIndex(), access->getOriginator());
		}

		return _transfersMap[sourceType][targetType](
			sourceMemoryPlace,
			targetMemoryPlace,
			region,
			access
		);
	}

	Step *WorkflowBase::createExecutionStep(Task *task, ComputePlace *computePlace)
	{
		switch(computePlace->getType()) {
			case nanos6_host_device:
				return new HostExecutionStep(task, computePlace);
			case nanos6_cluster_device:
				return new ClusterExecutionStep(task, computePlace);
			default:
				FatalErrorHandler::failIf(
					true,
					"Execution workflow does not support this device yet"
				);
				return nullptr;
		}
	}

	Step *WorkflowBase::createNotificationStep(
		std::function<void ()> const &callback,
		ComputePlace const *computePlace
	) {
		const nanos6_device_t type =
			(computePlace == nullptr) ? nanos6_host_device : computePlace->getType();

		switch (type) {
			case nanos6_host_device:
				return new HostNotificationStep(callback);
			case nanos6_cluster_device:
				return new ClusterNotificationStep(callback);
			default:
				FatalErrorHandler::failIf(
					true,
					"Execution workflow does not support this device yet"
				);
				//! Silencing annoying compiler warning
				return nullptr;
		}
	}

	Step *WorkflowBase::createDataReleaseStep(Task const *task, DataAccess *access)
	{
		if (task->isRemoteTask()) {
			return new ClusterDataReleaseStep(task->getClusterContext(), access);
		}

		return new DataReleaseStep(access);
	}


	void executeTask(Task *task, ComputePlace *targetComputePlace, MemoryPlace *targetMemoryPlace)
	{
		/* The workflow has already been created for this Task.
			* At this point the Task has been assigned to a WorkerThread
			* because all its pending DataCopy steps have been completed
			* and it's ready to actually run */
		if (task->getWorkflow() != nullptr) {
			ExecutionWorkflow::Step *executionStep = task->getExecutionStep();

			if (executionStep == nullptr) {
				/* Either this task has already executed and has been
				 * waiting for its children to complete or the task has
				 * no body (task->hasCode() is false).
				 *
				 * NOTE: in the former case, task->getWorkflow() is actually
				 *       a dangling pointer as the workflow has already been deleted.
				 *       In the latter it never gets deleted.
				 */

				assert (task->hasFinished());
				if (task->mustDelayRelease()) {

					WorkerThread *currThread = WorkerThread::getCurrentWorkerThread();
					CPU *cpu = currThread == nullptr ? nullptr : currThread->getComputePlace();

					if (task->markAllChildrenAsFinished(cpu)) {
					}
				}
				if (task->markAsReleased()) {
					TaskFinalization::disposeTask(task);
				}
			} else {
				executionStep->start();
			}

			return;
		}

		//! This is the target MemoryPlace that we will use later on,
		//! once the Task has completed, to update the location of its
		//! DataAccess objects. This can be overriden, if we
		//! release/unregister the accesses passing a different
		//! MemoryPlace.
		task->setMemoryPlace(targetMemoryPlace);

		// int numSymbols = task->getSymbolNum();
		Workflow<TaskExecutionWorkflowData> *workflow =
			createWorkflow<TaskExecutionWorkflowData>(0 /* numSymbols */);

		Step *executionStep = workflow->createExecutionStep(task, targetComputePlace);

		Step *notificationStep = workflow->createNotificationStep(
			[=]() {
				WorkerThread *currThread = WorkerThread::getCurrentWorkerThread();

				CPU * const cpu =
					(currThread != nullptr) ? currThread->getComputePlace() : nullptr;

				CPUDependencyData localDependencyData;
				CPUDependencyData &hpDependencyData =
					(cpu == nullptr) ? localDependencyData : cpu->getDependencyData();

				if (task->markAsFinished(cpu/* cpu */)) {
					DataAccessRegistration::unregisterTaskDataAccesses(
						task,
						cpu, /*cpu, */
						hpDependencyData,
						targetMemoryPlace
					);

					TaskFinalization::taskFinished(task, cpu);
					if (task->markAsReleased()) {
						TaskFinalization::disposeTask(task);
					}
				}

				delete workflow;
			},
			targetComputePlace
		);

		/* TODO: Once we have correct management for the Task symbols here
			* we should create the corresponding allocation steps. */

		DataAccessRegistration::processAllDataAccesses(
			task,
			[&](DataAccess *dataAccess) -> bool {
				assert(dataAccess != nullptr);
				DataAccessRegion const &region = dataAccess->getAccessRegion();

				MemoryPlace const *currLocation = dataAccess->getLocation();

				if (ClusterManager::inClusterMode()
					&& Directory::isDirectoryMemoryPlace(currLocation)
					&& targetComputePlace->getType() == nanos6_host_device) {

					Directory::HomeNodesArray const *homeNodes = Directory::find(region);

					for (const auto &entry : *homeNodes) {
						MemoryPlace const *entryLocation = entry->getHomeNode();
						const DataAccessRegion subregion = region.intersect(entry->getAccessRegion());

						Step *dataCopySubregionStep = workflow->createDataCopyStep(
							entryLocation,
							targetMemoryPlace,
							subregion,
							dataAccess
						);

						workflow->enforceOrder(dataCopySubregionStep, executionStep);
						workflow->addRootStep(dataCopySubregionStep);
					}

					delete homeNodes;

				} else {

					Step *dataCopyRegionStep = workflow->createDataCopyStep(
						currLocation,
						targetMemoryPlace,
						region,
						dataAccess
					);

					workflow->enforceOrder(dataCopyRegionStep, executionStep);
					workflow->addRootStep(dataCopyRegionStep);
				}

				Step *releaseStep = workflow->createDataReleaseStep(task, dataAccess);
				workflow->enforceOrder(executionStep, releaseStep);
				workflow->enforceOrder(releaseStep, notificationStep);

				return true;
			}
		);

		if (executionStep->ready()) {
			workflow->enforceOrder(executionStep, notificationStep);
			workflow->addRootStep(executionStep);
		}

		task->setWorkflow(workflow);
		task->setComputePlace(targetComputePlace);

		//! Starting the workflow will either execute the task to
		//! completion (if there are not pending transfers for the
		//! task), or it will setup all the Execution Step will
		//! execute when ready.
		workflow->start();
	}

	void setupTaskwaitWorkflow(Task *task, DataAccess *taskwaitFragment)
	{
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();

		ComputePlace const *computePlace =
			(currentThread == nullptr) ? nullptr : currentThread->getComputePlace();;

		Workflow<DataAccessRegion> *workflow = createWorkflow<DataAccessRegion>();

		DataAccessRegion region = taskwaitFragment->getAccessRegion();

		Step *notificationStep =
			workflow->createNotificationStep(
				[=]() {
					/* We cannot re-use the 'computePlace', we need to
						* retrieve the current Thread and associated
						* ComputePlace */
					WorkerThread *releasingThread = WorkerThread::getCurrentWorkerThread();

					ComputePlace *releasingComputePlace =
						(releasingThread == nullptr) ? nullptr : releasingThread->getComputePlace();

					/* Here, we are always using a local CPUDependencyData
						* object, to avoid the issue where we end-up calling
						* this while the thread is already in the dependency
						* system, using the CPUDependencyData of its
						* ComputePlace. This is a *TEMPORARY* solution, until
						* we fix how we handle taskwaits in a more clean
						* way. */
					CPUDependencyData localDependencyData;

					DataAccessRegistration::releaseTaskwaitFragment(
						task,
						region,
						releasingComputePlace,
						localDependencyData
					);

					delete workflow;
				},
				computePlace
			);

		MemoryPlace const *currLocation = taskwaitFragment->getLocation();
		MemoryPlace const *targetLocation = taskwaitFragment->getOutputLocation();

		//! No need to perform any copy for this taskwait fragment
		if (targetLocation == nullptr) {
			workflow->addRootStep(notificationStep);
			workflow->start();
			return;
		}

		Step *copyStep = workflow->createDataCopyStep(
			currLocation,
			targetLocation,
			region,
			taskwaitFragment
		);

		workflow->addRootStep(copyStep);
		workflow->enforceOrder(copyStep, notificationStep);
		workflow->start();
	}
};
