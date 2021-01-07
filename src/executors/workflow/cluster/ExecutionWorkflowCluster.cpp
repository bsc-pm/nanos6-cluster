/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "ExecutionWorkflowCluster.hpp"
#include "tasks/Task.hpp"

#include "lowlevel/SpinLock.hpp"
#include <ClusterManager.hpp>
#include <ClusterServicesPolling.hpp>
#include <ClusterServicesTask.hpp>
#include <DataAccess.hpp>
#include <DataAccessRegistration.hpp>
#include <DataTransfer.hpp>
#include <Directory.hpp>
#include <InstrumentLogMessage.hpp>
#include <TaskOffloading.hpp>
#include "InstrumentCluster.hpp"

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

			// namespacePredecessor is in principle irrelevant, because it only matters when the
			// task is created, not when a satisfiability message is sent (which is what is
			// happening now). Nevertheless, this only happens when propagation does not happen
			// in the namespace; so send the value nullptr.
			TaskOffloading::SatisfiabilityInfo satInfo(region, locationIndex, read, write, /* namespacePredecessor */ nullptr);

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

			/*
			 * If two tasks A and B are offloaded to the same namespace,
			 * and A has an in dependency, then at the remote side, read
			 * satisfiability is propagated from A to B both via the
			 * offloader's dependency system and via remote propagation in
			 * the remote namespace (this is the normal optimization from a
			 * namespace). So task B receives read satisfiability twice.
			 * This is harmless.  TODO: It would be good to add an
			 * assertion to make sure that read satisfiability arrives
			 * twice only in the circumstance described here, but we can no
			 * longer check what type of dependency the access of task A
			 * had (in fact the access may already have been deleted). This
			 * info would have to be added to the UpdateOperation.
			 *
			 * This unfortunately messes up the counting of linked bytes.
			 * The master should be able to figure out when this happens
			 * and update its own _bytesToLink value accordingly. Or
			 * maybe there is a way to avoid this happening.
			 */
			// delete this;
		}
	}

	void ClusterDataLinkStep::start()
	{
		bool deleteStep = false;
		{
			// Get a lock (see comment in ClusterDataLinkStep::linkRegion).
			std::lock_guard<SpinLock> guard(_lock);
			assert(_targetMemoryPlace != nullptr);

			int location = -1;
			if (_read || _write) {
				assert(_sourceMemoryPlace != nullptr);
				location = _sourceMemoryPlace->getIndex();
			}
			Instrument::logMessage(
					Instrument::ThreadInstrumentationContext::getCurrent(),
					"ClusterDataLinkStep for MessageTaskNew. ",
					"Current location of ", _region,
					" Node:", location
			);

			//! The current node is the source node. We just propagate
			//! the info we 've gathered
			assert(_successors.size() == 1);
			ClusterExecutionStep *execStep = dynamic_cast<ClusterExecutionStep *>(_successors[0]);
			assert(execStep != nullptr); // This asserts that the next step is the execution step

			// assert(_read || _write);

			// This assert is to prevent future errors when working with maleability.
			// For now the index and the comIndex are the same. A more complete implementation
			// Will have a pointer in ClusterMemoryNode to the ClusterNode and will get the
			// Commindex from there with getCommIndex.
			// assert(_sourceMemoryPlace->getIndex() == _sourceMemoryPlace->getCommIndex());
			execStep->addDataLink(location, _region, _read, _write, (void *)_namespacePredecessor);

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
			// TODO: See comment in linkRegion
			delete this;
		}
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
				_region,
				_targetMemoryPlace,
				_isTaskwait
			);
			releaseSuccessors();
			delete this;
			return;
		}


		Instrument::logMessage(
			Instrument::ThreadInstrumentationContext::getCurrent(),
			"ClusterDataCopyStep for:", _region,
			" from Node:", _sourceMemoryPlace->getIndex(),
			" to Node:", _targetMemoryPlace->getIndex()
		);

		DataTransfer *dt = ClusterManager::fetchData(_region, _sourceMemoryPlace);

		dt->addCompletionCallback(
			[&]() {
				Instrument::clusterDataReceived(
					_region.getStartAddress(),
					_region.getSize(),
					_sourceMemoryPlace->getIndex()
				);
				//! If this data copy is performed for a taskwait we
				//! don't need to update the location here.
				DataAccessRegistration::updateTaskDataAccessLocation(
					_task,
					_region,
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
		bool write,
		void *namespacePredecessor
	) {
		// This lock should already have been taken by the caller
		// Apparently it is not.
		//assert(_lock.isLockedByThisThread());
		_satInfo.push_back( TaskOffloading::SatisfiabilityInfo(region, source, read, write, namespacePredecessor) );
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
