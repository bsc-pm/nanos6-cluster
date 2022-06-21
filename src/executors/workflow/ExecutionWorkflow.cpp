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
#include "scheduling/Scheduler.hpp"

#include <ClusterManager.hpp>
#include <DataAccess.hpp>
#include <DataAccessRegistration.hpp>
#include <DataAccessRegistrationImplementation.hpp>
#include <ExecutionWorkflowHost.hpp>
#include <ExecutionWorkflowCluster.hpp>
#include <ClusterManager.hpp>
#include <InstrumentDependencySubsystemEntryPoints.hpp>
#include "CPUDependencyData.hpp"

namespace ExecutionWorkflow {

	transfers_map_t _transfersMap = {{
		/*              host         cuda      opencl    cluster   */
		/* host */    { nullCopy,    nullCopy, nullCopy, clusterCopy },
		/* cuda */    { nullCopy,    nullCopy, nullCopy, nullCopy },
		/* opencl */  { nullCopy,    nullCopy, nullCopy, nullCopy },
		/* cluster */ { clusterCopy, nullCopy, nullCopy, clusterCopy }
		}};

	Step *Workflow::createDataCopyStep(
		MemoryPlace const *sourceMemoryPlace,
		MemoryPlace const *targetMemoryPlace,
		DataAccessRegion const &region,
		DataAccess *access,
		bool isTaskwait,
		CPUDependencyData &hpDependencyData
	) {
		Step *step;
		Instrument::enterCreateDataCopyStep(isTaskwait);

		/* At the moment we do not support data copies for accesses
			* of the following types. This essentially mean that devices,
			* e.g. Cluster, CUDA, do not support these accesses. */
		if (access->getType() == REDUCTION_ACCESS_TYPE) {
			step = new Step();
			Instrument::exitCreateDataCopyStep(isTaskwait);
			return step;
		}

		assert(targetMemoryPlace != nullptr);
		assert(!targetMemoryPlace->isDirectoryMemoryPlace());

		// The source memory place is nullptr if and only if the dependency is
		// not yet read satisfied, which is only possible (at this point) if
		// the access is weak. If it is not read satisfied do nothing now:
		// don't copy the data and don't register the dependency. This means
		// for instance that the data will not be eagerly fetched (as
		// controlled by cluster.eager_weak_fetch) and the registration will be
		// done when we receive MessageSatisfiability.
		if (sourceMemoryPlace == nullptr) {
			assert(access->isWeak());
		}

		// Take the source memory type, except nullptr and the current memory node both
		// count as nanos6_host_device.
		MemoryPlace const *currentMemoryPlace = ClusterManager::getCurrentMemoryNode();
		nanos6_device_t sourceType =
			(sourceMemoryPlace == nullptr || sourceMemoryPlace == currentMemoryPlace)
					? nanos6_host_device : sourceMemoryPlace->getType();
		const nanos6_device_t targetType =
				(targetMemoryPlace == currentMemoryPlace)
					? nanos6_host_device : targetMemoryPlace->getType();

		/* Starting workflow for a task on the host: not in a namespace */
		if (targetType == nanos6_host_device) {
			access->setValidNamespaceSelf( ClusterManager::getCurrentMemoryNode()->getIndex());
		}

		if (sourceType == nanos6_host_device || sourceMemoryPlace == ClusterManager::getCurrentMemoryNode()) {
			if (access->getObjectType() == access_type && !access->isWeak() && access->getType() != READ_ACCESS_TYPE) {
				// Access already present, and the task will not modify the data (is weak or read-only access),
				// then register it as local in case it isn't already.
				access->setNewLocalWriteID();
			}
		}

		if (ClusterManager::inClusterMode() && sourceMemoryPlace->isDirectoryMemoryPlace()) {
			// In cluster mode, if it's in the directory, always use
			// clusterCopy. The data doesn't need copying, since being in the
			// directory implies that the data is uninitialized. But the new
			// location may need registering in the remote dependency system.
			step = clusterCopy(sourceMemoryPlace, targetMemoryPlace, region, access, hpDependencyData);
		} else {
			step = _transfersMap[sourceType][targetType](
				sourceMemoryPlace,
				targetMemoryPlace,
				region,
				access,
				hpDependencyData
			);
		}

		Instrument::exitCreateDataCopyStep(isTaskwait);
		return step;
	}

	Step *Workflow::createExecutionStep(Task *task, ComputePlace *computePlace)
	{
		switch(computePlace->getType()) {
			case nanos6_host_device:
				return new HostExecutionStep(task, computePlace);
			case nanos6_cluster_device:
				return new ClusterExecutionStep(task, computePlace);
			default:
				FatalErrorHandler::fail("Execution workflow does not support this device yet");
				return nullptr;
		}
	}

	DataReleaseStep *Workflow::createDataReleaseStep(Task *task)
	{
		if (task->isRemoteTask()) {
			return new ClusterDataReleaseStep(task->getClusterContext(), task);
		}

		return new DataReleaseStep(task);
	}


	void Workflow::start()
	{
		std::map<MemoryPlace const*, size_t> fragments;
		std::map<MemoryPlace const*, std::vector<ClusterDataCopyStep *>> groups;

		// Iterate over all the rootSteps. There will be null copies
		for (Step *step : _rootSteps) {

			ClusterDataCopyStep *clusterCopy = dynamic_cast<ClusterDataCopyStep *>(step);

			// It is a null copy or some other type.
			if (!clusterCopy) {
				step->start();
				continue;
			}

			// It is a copy step, so group them respect to destination
			// requiresDataFetch will inmediately release successors when
			// (!_needsTransfer && !_isTaskwait)
			if (clusterCopy->requiresDataFetch()) {
				assert(clusterCopy->getTargetMemoryPlace()
					== ClusterManager::getCurrentMemoryNode());

				MemoryPlace const* source = clusterCopy->getSourceMemoryPlace();

				fragments[source] += clusterCopy->getNumFragments();
				groups[source].push_back(clusterCopy);
			}
		}

		for (auto const& it : groups) {
			MemoryPlace const* source = it.first;

			// Instrument::logMessage(
			// 	Instrument::ThreadInstrumentationContext::getCurrent(),
			// 	"ClusterDataCopyStep for:", _region,
			// 	" from Node:", source,
			// 	" to Node:", ClusterManager::getCurrentMemoryNode()
			// );

			ClusterManager::fetchVector(fragments[source], it.second, source);
		}
	}



	void executeTask(Task *task, ComputePlace *targetComputePlace, MemoryPlace *targetMemoryPlace)
	{
		/* The workflow has already been created for this Task.
		 * At this point the Task has been assigned to a WorkerThread
		 * because all its pending DataCopy steps have been completed
		 * and it's ready to actually run.
		 */
#ifndef NDEBUG
		// This will expose some nasty errors difficult to debug latter. TaskforSource only comes
		// here twise. When creating the workflow and when deleting the executionStep and releasing
		// the notificationStep.
		if (task->isTaskforSource() && task->getWorkflow() != nullptr) {
			assert(task->getExecutionStep() != nullptr);
		}
#endif

		if (task->getWorkflow() != nullptr) {
			ExecutionWorkflow::Step *executionStep = task->getExecutionStep();

			if (executionStep == nullptr) {

				/* Task has already executed and is in a "wait" clause waiting
				 * for its children to complete. The notification step has
				 * already been executed, but markAsFinished returned false.
				 * Now, finally, the wait clause is done, the accesses can be
				 * unregistered and the task disposed. NOTE:
				 * task->getWorkflow() is actually a dangling pointer as the
				 * workflow has already been deleted.
				 */

				assert(task->mustDelayRelease());
				WorkerThread const *currThread = WorkerThread::getCurrentWorkerThread();
				CPU * const cpu =
					(currThread == nullptr) ? nullptr : currThread->getComputePlace();
				CPUDependencyData localDependencyData;
				CPUDependencyData &hpDependencyData =
					(cpu == nullptr) ? localDependencyData : cpu->getDependencyData();

				/*
				 * Continue what was started in Task::markAsFinished, i.e.
				 * everything after Task::markAsBlocked returned false.
				 */
				task->completeDelayedRelease();
				task->markAsUnblocked();
				DataAccessRegistration::handleExitTaskwait(task, cpu, hpDependencyData);

				/*
				 * Now finish the notification step, i.e. everything after
				 * Task::markAsFinished returned false, except that the work
				 * of TaskFinalization::taskFinished(task, cpu) was already done
				 * when a child finished and called TaskFinalization::taskFinished.
				 */
				assert (task->hasFinished());
				DataAccessRegistration::unregisterTaskDataAccesses(
					task,
					cpu, /*cpu, */
					hpDependencyData,
					targetMemoryPlace,
					false,
					/* For clusters, finalize this task and send
					 * the MessageTaskFinished BEFORE propagating
					 * satisfiability to any other tasks. This is to
					 * avoid potentially sending the
					 * MessageTaskFinished messages out of order
					 */
					[&]() -> void {
						TaskFinalization::taskFinished(task, cpu);
						if (task->markAsReleased()) {
							TaskFinalization::disposeTask(task);
						}
					}
				);

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
		Workflow *workflow = new Workflow();

		Step *executionStep = workflow->createExecutionStep(task, targetComputePlace);

		Step *notificationStep = nullptr;


		if (task->isTaskforCollaborator()) {
			// Now we only support host's taskfor because they are not offloaded (yet).
			assert(targetComputePlace->getType() == nanos6_host_device);

			// For collaborators don't go to the Dependency System. It is simpler as they don't have
			// dependencies.
			notificationStep = new NotificationStep(
				[task]() -> void {
					WorkerThread *currThread = WorkerThread::getCurrentWorkerThread();
					CPU * const cpu = (currThread == nullptr) ? nullptr : currThread->getComputePlace();

					if (task->markAsFinished(cpu)) {
						TaskFinalization::taskFinished(task, cpu);
						if (task->markAsReleased()) {
							TaskFinalization::disposeTask(task);
						}
					}
				}
			);
		} else {
			// At the moment we only support host and cluster devices.
			assert(targetComputePlace->getType() == nanos6_host_device
				||targetComputePlace->getType() == nanos6_cluster_device);

			notificationStep = new NotificationStep(
				[task, targetComputePlace, targetMemoryPlace]() -> void {
					WorkerThread *currThread = WorkerThread::getCurrentWorkerThread();
					CPU * const cpu = (currThread == nullptr) ? nullptr : currThread->getComputePlace();

					// For offloaded tasks with cluster.disable_autowait=false, handle
					// the early release of dependencies propagated in the namespace. All
					// other dependencies will be handled using the normal "wait" mechanism.
					CPUDependencyData localDependencyData;
					CPUDependencyData &hpDependencyData =
						(cpu == nullptr) ? localDependencyData : cpu->getDependencyData();

					DataAccessRegistration::unregisterLocallyPropagatedTaskDataAccesses(
						task,
						cpu,
						hpDependencyData);

					if (task->markAsFinished(cpu)) {
						DataAccessRegistration::unregisterTaskDataAccesses(
							task,
							cpu, /*cpu, */
							hpDependencyData,
							targetMemoryPlace,
							false,
							// For clusters, finalize this task and send the MessageTaskFinished
							// BEFORE propagating satisfiability to any other tasks. This is to
							// avoid potentially sending the MessageTaskFinished messages out of
							// order
							[&]() -> void {
								TaskFinalization::taskFinished(task, cpu);
								if (task->markAsReleased()) {
									TaskFinalization::disposeTask(task);
								}
							}
						);

					}
				}
			);
		}

		// TODO: Once we have correct management for the Task symbols here we should create the
		// corresponding allocation steps.
		DataReleaseStep *releaseStep = workflow->createDataReleaseStep(task);
		workflow->enforceOrder(executionStep, releaseStep);
		workflow->enforceOrder(releaseStep, notificationStep);

		// We must use local dependency data here, not the CPU's dependency data. This is because
		// we may currently be creating the workflow for an offloaded task, which happens inside
		// functions called (indirectly) by DataRegistration::processSatisfiedOriginators.
		// Note: it is only creating the workflow for an offloaded task that happens immediately,
		// never the execution of a non-offloaded task. So the CPU dependency data may still be
		// being used and we cannot use it again! Strictly speaking it is only the satisfied
		// originators that are still in use, but (1) we need these, because if the task's data
		// is found by the WriteID, then the data copy step will call setLocationFromWorkflow,
		// which may indeed create satisfied originators; and (2) it is dangerous to rely on the
		// fact that DataRegistration::processSatisfiedOriginators is the last to be called in
		// DataAccessRegistration::processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks,
		// and therefore the rest of the CPU dependency data isn't needed.
		CPUDependencyData localDependencyData2;
		// Thread and CPU while creating the workflow (not when executing the lambda functions)
		// WorkerThread *createCurrThread = WorkerThread::getCurrentWorkerThread();
		// CPU * const createCpu = (createCurrThread == nullptr) ? nullptr : createCurrThread->getComputePlace();
		// CPUDependencyData &hpDependencyData2 =
		//	(createCpu == nullptr) ? localDependencyData2 : createCpu->getDependencyData();
		CPUDependencyData &hpDependencyData2 = localDependencyData2;

		DataAccessRegistration::processAllDataAccesses(
			task,
			[&](DataAccess *dataAccess) -> bool {
				assert(dataAccess != nullptr);
				DataAccessRegion const &region = dataAccess->getAccessRegion();

				MemoryPlace const *currLocation = dataAccess->getLocation();

#ifndef NDEBUG
				// In debug mode, raise an error if the task has a non-weak access to
				// an unknown region.
				if (!dataAccess->isWeak()
					&& ClusterManager::inClusterMode()
					&& Directory::isDirectoryMemoryPlace(currLocation)
					&& targetComputePlace->getType() == nanos6_host_device) {

					// This isn't perfect, because the homeNodes list is only empty if the whole
					// region is missing from the directory whereas we would prefer to raise an
					// error even if just a part of it is missing. But this test does a good job
					// of finding blatantly wrong accesses.
					Directory::HomeNodesArray const *homeNodes = Directory::find(region);
					FatalErrorHandler::failIf(
						homeNodes->empty(),
						"Non-weak access ",
						region,
						" of ",
						task->getLabel(),
						" is an unknown region not from lmalloc, dmalloc or the stack");

					delete homeNodes;
				}
#endif
				Step *dataCopyRegionStep = workflow->createDataCopyStep(
					currLocation,
					targetMemoryPlace,
					region,
					dataAccess,
					false,
					hpDependencyData2
				);

				workflow->enforceOrder(dataCopyRegionStep, executionStep);
				workflow->addRootStep(dataCopyRegionStep);

				releaseStep->addAccess(dataAccess);

				return true;
			}
		);


		if (executionStep->ready()) {
			workflow->enforceOrder(executionStep, notificationStep);
			workflow->addRootStep(executionStep);
		}

		task->setWorkflow(workflow);
		task->setComputePlace(targetComputePlace);

		// There may be some delayed operations from setLocationFromWorkflow, which
		// is called when a data transfer is not created because it is found by
		// WriteID.
		WorkerThread const *currThread = WorkerThread::getCurrentWorkerThread();
		CPU * const cpu =
			(currThread == nullptr) ? nullptr : currThread->getComputePlace();

		// Two things may have created delayed operations:
		// (1) Setting the namespace for this task's accesses may require passing
		// the valid namespace information to the successor accesses.
		// (2) Updating the data location if found by WriteID.
		// We couldn't do either while holding the lock on our task's access
		// structures (taken by DataAccessRegistration::processAllDataAccesses),
		// so do all the delayed operations now.
		// Do this BEFORE starting the workflow, as the access could otherwise be
		// removed before getting the namespace info.
		DataAccessRegistration::processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(hpDependencyData2, cpu, false);

		// Starting the workflow will either execute the task to
		// completion (if there are not pending transfers for the
		// task), or it will setup all the Execution Step will
		// execute when ready.
		workflow->start();
	}

	void setupTaskwaitWorkflow(
		Task *task,
		DataAccess *taskwaitFragment,
		CPUDependencyData &hpDependencyData
	) {
		Instrument::enterSetupTaskwaitWorkflow();
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();

		ComputePlace *computePlace =
			(currentThread == nullptr) ? nullptr : currentThread->getComputePlace();;

		DataAccessRegion region = taskwaitFragment->getAccessRegion();

		MemoryPlace const *targetLocation = taskwaitFragment->getOutputLocation();

		//! No need to perform any copy for this taskwait fragment
		if (targetLocation == nullptr) {
			DataAccessRegistration::releaseTaskwaitFragment(
				task,
				region,
				computePlace,
				hpDependencyData,
				false);
			Instrument::exitSetupTaskwaitWorkflow();
			return;
		}

		Workflow *workflow = new Workflow();


		Step *notificationStep = new NotificationStep(
			[task, region, workflow]() -> void {
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
					localDependencyData,
					true
				);

				delete workflow;
			}
		);

		MemoryPlace const *currLocation = taskwaitFragment->getLocation();

		Step *copyStep = workflow->createDataCopyStep(
			currLocation,
			targetLocation,
			region,
			taskwaitFragment,
			true,
			hpDependencyData
		);

		workflow->addRootStep(copyStep);
		workflow->enforceOrder(copyStep, notificationStep);
		workflow->start();
		Instrument::exitSetupTaskwaitWorkflow();
	}

};
