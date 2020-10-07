/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "ExecutionWorkflowCluster.hpp"
#include "tasks/Task.hpp"

#include <ClusterManager.hpp>
#include <ClusterServicesPolling.hpp>
#include <ClusterServicesTask.hpp>
#include <DataAccess.hpp>
#include <DataAccessRegistration.hpp>
#include <DataTransfer.hpp>
#include <Directory.hpp>
#include <InstrumentLogMessage.hpp>
#include <TaskOffloading.hpp>

namespace ExecutionWorkflow {

	void ClusterDataLinkStep::linkRegion(
		DataAccessRegion const &region,
		MemoryPlace const *location,
		bool read,
		bool write
	) {
		bool deleteStep = false;
		// This function is occasionally called after creating the
		// ClusterDataLinkStep (whose constructor sets the data link
		// step) but before starting the ClusterDataLinkStep. A lock
		// is required because both functions manipulate _bytesToLink 
		// and delete the workflow when it reaches zero.
		{
			std::lock_guard<SpinLock> guard(_lock);
			assert(_targetMemoryPlace != nullptr);

			int locationIndex;
			if (location == nullptr) {
				// The location is only nullptr when write satisfiability
				// is propagated before read satisfiability, which happens
				// very rarely. In this case, we send -1 as the location
				// index.
				assert(write);
				assert(!read);
				locationIndex = -1; // means nullptr
			} else {
				if (location->getType() != nanos6_cluster_device) {
					location = ClusterManager::getCurrentMemoryNode();
				}
				locationIndex = location->getIndex();
			}

			TaskOffloading::SatisfiabilityInfo satInfo(region, locationIndex, read, write);

			TaskOffloading::ClusterTaskContext *clusterTaskContext = _task->getClusterContext();
			TaskOffloading::sendSatisfiability(_task, clusterTaskContext->getRemoteNode(), satInfo);
			size_t linkedBytes = region.getSize();

			//! We need to account for linking both read and write satisfiability
			if (read && write) {
				linkedBytes *= 2;
			}
			_bytesToLink -= linkedBytes;
			if (_started && _bytesToLink  == 0) {
				deleteStep = true;
			}
		}
		if (deleteStep) {
			delete this;
		}
	}

	void ClusterDataLinkStep::start()
	{
		bool deleteStep = false;
		{
			// Get a lock (see comment in ClusterDataLinkStep::linkRegion).
			std::lock_guard<SpinLock> guard(_lock);
			assert(_targetMemoryPlace != nullptr);

			if (!_read && !_write) {
				//! Nothing to do here. We can release the execution
				//! step. Location will be linked later on.
				releaseSuccessors();
				return;
			}

			assert(_sourceMemoryPlace != nullptr);
			Instrument::logMessage(
					Instrument::ThreadInstrumentationContext::getCurrent(),
					"ClusterDataLinkStep for MessageTaskNew. ",
					"Current location of ", _region,
					" Node:", _sourceMemoryPlace->getIndex()
			);

			//! The current node is the source node. We just propagate
			//! the info we 've gathered
			assert(_successors.size() == 1);
			ClusterExecutionStep *execStep = (ClusterExecutionStep *)_successors[0];

			assert(_read || _write);
			execStep->addDataLink(_sourceMemoryPlace->getIndex(), _region, _read, _write);

			const size_t linkedBytes = _region.getSize();
			//! If at the moment of offloading the access is not both
			//! read and write satisfied, then the info will be linked
			//! later on. In this case, we just account for the bytes that
			//! we link now, the Step will be deleted when all the bytes
			//! are linked through linkRegion method invocation
			if (_read && _write) {
				deleteStep = true;
			} else {
				_bytesToLink -= linkedBytes;
				_started = true;
			}

			// Release successors before releasing the lock (otherwise
			// ClusterDataLinkStep::linkRegion may delete this step first).
			releaseSuccessors();
		}

		if (deleteStep) {
			delete this;
		}
	}

	void ClusterDataReleaseStep::releaseRegion(
		DataAccessRegion const &region,
		MemoryPlace const *location
	) {
		Instrument::logMessage(
			Instrument::ThreadInstrumentationContext::getCurrent(),
			"releasing remote region:",
			region
		);

		TaskOffloading::sendRemoteAccessRelease(
			_remoteTaskIdentifier,
			_offloader,
			region,
			_type,
			_weak,
			location
		);

		if ((_bytesToRelease -= region.getSize()) == 0) {
			delete this;
		}
	}

	bool ClusterDataReleaseStep::checkDataRelease(DataAccess const *access)
	{
		const bool releases = (access->getObjectType() == taskwait_type)
			&& access->getOriginator()->isSpawned()
			&& access->readSatisfied()
			&& access->writeSatisfied();

		Instrument::logMessage(
			Instrument::ThreadInstrumentationContext::getCurrent(),
			"Checking DataRelease access:", access->getInstrumentationId(),
			" object_type:", access->getObjectType(),
			" spawned originator:", access->getOriginator()->isSpawned(),
			" read:", access->readSatisfied(),
			" write:", access->writeSatisfied(),
			" releases:", releases
		);

		return releases;
	}

	void ClusterDataReleaseStep::start()
	{
		releaseSuccessors();
	}

	void ClusterDataCopyStep::start()
	{
		assert(ClusterManager::getCurrentMemoryNode() == _targetMemoryPlace);
		assert(_sourceMemoryPlace->getType() == nanos6_cluster_device);
		assert(_targetMemoryPlace->getType() == nanos6_cluster_device);

		//! No data transfer needed, data is already here.
		if (_sourceMemoryPlace == _targetMemoryPlace) {
			releaseSuccessors();
			delete this;
			return;
		}

		if (!_needsTransfer && !_isTaskwait) {
			//! Perform the data access registration but not the data
			//! fetch.
			DataAccessRegistration::updateTaskDataAccessLocation(
				_task,
				_targetTranslation._hostRegion,
				_targetMemoryPlace,
				_isTaskwait
			);
			releaseSuccessors();
			delete this;
			return;
		}


		Instrument::logMessage(
			Instrument::ThreadInstrumentationContext::getCurrent(),
			"ClusterDataCopyStep for:", _targetTranslation._hostRegion,
			" from Node:", _sourceMemoryPlace->getIndex(),
			" to Node:", _targetMemoryPlace->getIndex()
		);

		DataTransfer *dt = ClusterManager::fetchData(
			_targetTranslation._hostRegion,
			_sourceMemoryPlace
		);

		dt->setCompletionCallback(
			[&]() {
				//! If this data copy is performed for a taskwait we
				//! don't need to update the location here.
				DataAccessRegistration::updateTaskDataAccessLocation(
					_task,
					_targetTranslation._hostRegion,
					_targetMemoryPlace,
					_isTaskwait
				);
				this->releaseSuccessors();
				delete this;
			}
		);

		ClusterPollingServices::PendingQueue<DataTransfer>::addPending(dt);
	}

	ClusterExecutionStep::ClusterExecutionStep(Task *task, ComputePlace *computePlace)
		: Step(),
		_satInfo(),
		_remoteNode(reinterpret_cast<ClusterNode *>(computePlace)),
		_task(task)
	{
		assert(computePlace->getType() == nanos6_cluster_device);

		TaskOffloading::ClusterTaskContext *clusterContext =
			new TaskOffloading::ClusterTaskContext((void *)_task, _remoteNode);
		_task->setClusterContext(clusterContext);
	}

	void ClusterExecutionStep::addDataLink(
		int source,
		DataAccessRegion const &region,
		bool read,
		bool write
	) {
                // This lock should already have been taken by the caller
		// std::lock_guard<SpinLock> guard(_lock);
		_satInfo.push_back( TaskOffloading::SatisfiabilityInfo(region, source, read, write) );
	}

	void ClusterExecutionStep::start()
	{
		_task->setExecutionStep(this);
		TaskOffloading::offloadTask(_task, _satInfo, _remoteNode);
	}

	void ClusterNotificationStep::start()
	{
		if (_callback) {
			_callback();
		}

		releaseSuccessors();
		delete this;
	}
};
