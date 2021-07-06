/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef EXECUTION_WORKFLOW_CLUSTER_HPP
#define EXECUTION_WORKFLOW_CLUSTER_HPP

#include <functional>

#include "../ExecutionStep.hpp"

#include <ClusterManager.hpp>
#include <ClusterTaskContext.hpp>
#include <DataAccess.hpp>
#include <Directory.hpp>
#include <InstrumentLogMessage.hpp>
#include <SatisfiabilityInfo.hpp>
#include <TaskOffloading.hpp>
#include <VirtualMemoryManagement.hpp>
#include <tasks/Task.hpp>
#include <ClusterUtil.hpp>
#include <DataAccessRegistration.hpp>

#include <MessageReleaseAccess.hpp>

class ComputePlace;
class MemoryPlace;

namespace ExecutionWorkflow {
	class ClusterDataLinkStep : public DataLinkStep {
		//! The MemoryPlace that holds the data at the moment
		MemoryPlace const * const _sourceMemoryPlace;

		//! The MemoryPlace that requires the data
		MemoryPlace const * const _targetMemoryPlace;

		//! DataAccessRegion that the Step covers
		const DataAccessRegion _region;

		//! The task in which the access belongs to
		Task * const _task;

		//! read satisfiability at creation time
		bool _read;

		//! write satisfiability at creation time
		bool _write;

		Task *_namespacePredecessor;
		WriteID _writeID;

		bool _started;

	public:
		ClusterDataLinkStep(
			MemoryPlace const *sourceMemoryPlace,
			MemoryPlace const *targetMemoryPlace,
			DataAccess *access
		) : DataLinkStep(access),
			_sourceMemoryPlace(sourceMemoryPlace),
			_targetMemoryPlace(targetMemoryPlace),
			_region(access->getAccessRegion()),
			_task(access->getOriginator()),
			_read(access->readSatisfied()),
			_write(access->writeSatisfied()),
			_namespacePredecessor(nullptr),
			_writeID(access->getWriteID()),
			_started(false)
		{
			access->setDataLinkStep(this);

			// Data link for read access. Ignore true
			// write satisfiability and send only pseudowrite
			// satisfiability if read satisfied.
			if (access->getType() == READ_ACCESS_TYPE) {
				access->setRemoteHasPseudowrite();
				_write = false;
				if (_read) {
					_write = true;
				}
			}

			assert(targetMemoryPlace->getType() == nanos6_device_t::nanos6_cluster_device);
			const int targetNamespace = targetMemoryPlace->getIndex();

			/* Starting workflow on another node: set the namespace and predecessor task */
			if (ClusterManager::getDisableRemote()) {
				_namespacePredecessor = nullptr;
			} else {
				if (access->getValidNamespacePrevious() == targetNamespace) {
					_namespacePredecessor = access->getNamespacePredecessor(); // remote propagation valid if predecessor task and offloading node matches
				} else {
					_namespacePredecessor = nullptr;
				}
			}

			DataAccessRegistration::setNamespaceSelf(access, targetNamespace);
		}

		void linkRegion(DataAccess const *region, bool read, bool write) override;

		//! Start the execution of the Step
		void start() override;
	};

	class ClusterDataCopyStep : public Step {
		//! The MemoryPlace that the data will be copied from.
		MemoryPlace const * const _sourceMemoryPlace;

		//! The MemoryPlace that the data will be copied to.
		MemoryPlace const * const _targetMemoryPlace;

		//! A mapping of the address range in the source node to the target node.
		DataAccessRegion const _fullRegion;
		std::vector<DataAccessRegion> _regionsFragments;

		//! The task on behalf of which we perform the data copy
		Task * const _task;

		const WriteID _writeID;

		//! The data copy is for a taskwait
		const bool _isTaskwait;

		//! The access is weak
		const bool _isWeak;

		//! An actual data transfer is required
		const bool _needsTransfer;

		const bool _registerLocation;

		//! Number of fragments messages
		size_t _nFragments;

		DataTransfer::data_transfer_callback_t _postcallback;

	public:
		ClusterDataCopyStep(
			MemoryPlace const *sourceMemoryPlace,
			MemoryPlace const *targetMemoryPlace,
			DataAccessRegion const &region,
			Task *task,
			WriteID writeID,
			bool isTaskwait,
			bool isWeak,
			bool needsTransfer,
			bool registerLocation
		);

		//! Start the execution of the Step
		void start() override
		{
		};

		bool requiresDataFetch();

		MemoryPlace const *getSourceMemoryPlace() const
		{
			return _sourceMemoryPlace ;
		}

		MemoryPlace const *getTargetMemoryPlace() const
		{
			return _targetMemoryPlace ;
		}

		size_t getNumFragments() const
		{
			return _nFragments;
		}

		const std::vector<DataAccessRegion> &getFragments() const
		{
			return _regionsFragments;
		}

		DataTransfer::data_transfer_callback_t getPostCallback() const
		{
			return _postcallback;
		}
	};

	class ClusterDataReleaseStep : public DataReleaseStep {
		//! identifier of the remote task
		void *_remoteTaskIdentifier;

		//! the cluster node we need to notify
		ClusterNode const *_offloader;

		MessageReleaseAccess::ReleaseAccessInfoVector _releaseInfo;

		MessageReleaseAccess *releaseInfoVectorToMessage()
		{
#ifndef NDEBUG
			assert(_releaseInfo.size() > 0);
#endif // NDEBUG

			MessageReleaseAccess *msg = new MessageReleaseAccess(
				ClusterManager::getCurrentClusterNode(),
				_remoteTaskIdentifier,
				_releaseInfo
			);

			_releaseInfo.clear();

			return msg;
		}

	public:
		ClusterDataReleaseStep(TaskOffloading::ClusterTaskContext *context, Task *task)
			: DataReleaseStep(task),
			_remoteTaskIdentifier(context->getRemoteIdentifier()),
			_offloader(context->getRemoteNode())
		{
			task->setDataReleaseStep(this);
		}

		virtual inline ~ClusterDataReleaseStep()
		{
			assert(_releaseInfo.size() == 0);
		}

		void addAccess(DataAccess *access)
		{
			_bytesToRelease.fetch_add(access->getAccessRegion().getSize());
		}

		void releasePendingAccesses() override
		{
			_infoLock.lock();
			// check atomically without taking the lock in case we can return immediately faster.
			if (_releaseInfo.size() == 0) {
				_infoLock.unlock();
				return;
			}

			// releaseInfoVectorToMessage takes the lock, but we don't need it calling sendMessage.
			// But when_bytesToRelease we call the destructor and both situations are very
			// frequent.
			MessageReleaseAccess *msg = releaseInfoVectorToMessage();
			_infoLock.unlock();

			ClusterManager::sendMessage(msg, _offloader);
		}

		void addToReleaseList(DataAccess const *access) override
		{
			assert(access->getLocation() != nullptr);

			DataAccessRegion const &region = access->getAccessRegion();
			MemoryPlace const *location = access->getLocation();
			const size_t size = region.getSize();

			Instrument::logMessage(
				Instrument::ThreadInstrumentationContext::getCurrent(),
				"addToReleaseList remote region:", region
			);

			// If location is a host device on this node it is a cluster
			// device from the point of view of the remote node
			const MemoryPlace * const clusterLocation =
				(location->getType() == nanos6_cluster_device || location->isDirectoryMemoryPlace())
				? location
				: ClusterManager::getCurrentMemoryNode();

			{  // scope for the lock to avoid errors with lock/unlock calls..
				std::lock_guard<SpinLock> lck(_infoLock);

				_releaseInfo.push_back(
					MessageReleaseAccess::ReleaseAccessInfo(
						region,
						access->getWriteID(),
						clusterLocation)
				);

				// (var.fetch_sub(arg) - arg) === (var -= arg) I prefer fetch_sub to remember it is
				// atomic.
				if (_bytesToRelease.fetch_sub(size) - size == 0) {
					MessageReleaseAccess * msg = releaseInfoVectorToMessage();

					// This is a call to mpi_Isend with the lock taken. We try to avoid this as much
					// as possible, so we only call it when we intent to call the destructor.
					ClusterManager::sendMessage(msg, _offloader);

					_task->unsetDataReleaseStep();
					delete this;
				}
			}  // end of locked protected region here.
		}


		bool checkDataRelease(DataAccess const *access) const override
		{
			Task const * const task = access->getOriginator();

			const bool mustWait = task->mustDelayRelease() && !task->allChildrenHaveFinished();

			const bool releases = ( (access->getObjectType() == taskwait_type) // top level sink
			                        || !access->hasSubaccesses()) // or no fragments (i.e. no subtask to wait for)
				&& task->hasFinished()     // must have finished; i.e. not taskwait inside task
				&& access->readSatisfied() && access->writeSatisfied()
				&& access->getOriginator()->isRemoteTask()  // only offloaded tasks: necessary (e.g. otherwise taskwait on will release)
				&& access->complete()                       // access must be complete
				&& !access->hasNext()                       // no next access at the remote side
				&& !mustWait;

			Instrument::logMessage(
				Instrument::ThreadInstrumentationContext::getCurrent(),
				"Checking DataRelease access:", access->getInstrumentationId(),
				" object_type:", access->getObjectType(),
				" spawned originator:", access->getOriginator()->isSpawned(),
				" read:", access->readSatisfied(),
				" write:", access->writeSatisfied(),
				" complete:", access->complete(),
				" has-next:", access->hasNext(),
				" task finished:", task->hasFinished(),
				" releases:", releases
			);

			return releases;
		}

		void start() override
		{
			releaseSuccessors();
		}
	};

	class ClusterExecutionStep : public Step {
	private:
		std::vector<TaskOffloading::SatisfiabilityInfo> _satInfo;
		ClusterNode *_remoteNode;
		Task *_task;

	public:
		ClusterExecutionStep(Task *task, ComputePlace *computePlace)
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

		//! Inform the execution Step about the existence of a
		//! pending data copy.
		//!
		//! \param[in] source is the id of the MemoryPlace that the data
		//!            is currently located
		//! \param[in] region is the memory region being copied
		//! \param[in] size is the size of the region being copied.
		//! \param[in] read is true if access is read-satisfied
		//! \param[in] write is true if access is write-satisfied
		//! \param[in] namespacePredecessorId is nullptr or predecessor remote task ID
		void addDataLink(
			int source,
			DataAccessRegion const &region,
			WriteID writeID,
			bool read, bool write,
			void *namespacePredecessorId
		) {
			// This lock should already have been taken by the caller
			// Apparently it is not.
			//assert(_lock.isLockedByThisThread());
			_satInfo.push_back(
				TaskOffloading::SatisfiabilityInfo(
					region, source,
					read, write,
					writeID, namespacePredecessorId)
			);
		}

		//! Start the execution of the Step
		void start() override
		{
			_task->setExecutionStep(this);
			TaskOffloading::offloadTask(_task, _satInfo, _remoteNode);
		}
	};

	inline Step *clusterFetchData(
		MemoryPlace const *source,
		MemoryPlace const *target,
		DataAccessRegion const &inregion,
		DataAccess *access
	) {
		assert(source != nullptr);
		assert(target == ClusterManager::getCurrentMemoryNode());

		const DataAccessType type = access->getType();
		const DataAccessRegion region = access->getAccessRegion();
		const DataAccessObjectType objectType = access->getObjectType();
		const bool isDistributedRegion = VirtualMemoryManagement::isDistributedRegion(region);
		const bool isWeak = access->isWeak();

		//! The source device is a host MemoryPlace of the current
		//! ClusterNode. We do not really need to perform a
		//! DataTransfer
		//! || The source and the destination is the same
		//! || I already have the data.
		if (source->isClusterLocalMemoryPlace()
			|| WriteIDManager::checkWriteIDLocal(access->getWriteID(), region)) {

			// NULL copy (do nothing, just release succesor and delete itself.)
			return new Step();
		}

		// Helpful warning messages in debug build

		if (region.getSize() > (1UL<<60)) {
			if (objectType == access_type && type != NO_ACCESS_TYPE && !isWeak) {
				FatalErrorHandler::fail(
					"Large access ", region,
					" for task ", access->getOriginator()->getLabel(),
					" is not weak"
				);
			}

			// With cluster.eager_weak_fetch = true, the following code is not valid
			//   int *a = (int *)nanos6_lmalloc(4);
			//   #pragma oss task weakinout(a[1]) label("T") {...}
			//   a[0] = 1;
			// This is because T will fetch a and return its location as node 1. So any
			// subsequent task will fetch a from node 1 and not return the data written
			// on node 0. This is probably acceptable in this example where the weakinout
			// is explicit, but when the weakinout is "all memory" there is too much
			// chance of this kind of thing happening.
			if (ClusterManager::getEagerWeakFetch()
				|| ClusterManager::getMessageMaxSize() != SIZE_MAX) {

				FatalErrorHandler::fail(
					"Set cluster.eager_weak_fetch = false and cluster.message_max_size = -1 for large weak memory access ",
					region,
					" of task ",
					access->getOriginator()->getLabel());
			}
		}

		bool needsTransfer =
			(
			 	//! We need a DataTransfer for a taskwait access
				//! in the following cases:
				//! 1) the access is not a NO_ACCESS_TYPE, so it
				//!    is part of the calling task's dependencies,
				//!    which means that the latest version of
				//!    the region needs to be present in the
				//!    context of the task at all times.
				//! 2) the access is a NO_ACCESS_TYPE access, so
				//!    it represents a region allocated within
				//!    the context of the Task but it is local
				//!    memory, so it needs to be present in the
				//!    context of the Task after the taskwait.
				//!    Distributed memory regions, do not need
				//!    to trigger a DataCopy, since anyway can
				//!    only be accessed from within subtasks.
				//!
				//! In both cases, we can avoid the copy if the
				//! access is a read-only access.
			 	(objectType == taskwait_type)
				&& (type != READ_ACCESS_TYPE)
				&& (type != NO_ACCESS_TYPE || !isDistributedRegion)
				&& !source->isDirectoryMemoryPlace()
			) || (
				//! We need a DataTransfer for an access_type
				//! access, if the access is not write-only
			 	(objectType == access_type)
				&& (type != WRITE_ACCESS_TYPE)
				//! and if it is not in the directory (which would mean
				//! that the data is not yet initialized)
				&& !source->isDirectoryMemoryPlace()
				//! and, if it is a weak access, then only
				//! if cluster.eager_weak_fetch == true
				&& (!isWeak || ClusterManager::getEagerWeakFetch())
			);

		//! If no data transfer is needed, then register the new location if
		//! it is a task with a non-weak access. This happens for out dependencies
		//! (WRITE_ACCESS_TYPE), because the task will write the new data contents.
		//! It also happens when the data was previously uninitialized (was in
		//! the directory) even on an in or inout dependency.
		bool registerLocation = !needsTransfer
			&& objectType != taskwait_type
			&& !isWeak;

		return new ClusterDataCopyStep(
			source, target, inregion,
			access->getOriginator(),
			access->getWriteID(),
			objectType == taskwait_type,
			isWeak,
			needsTransfer,
			registerLocation
		);
	}

	inline Step *clusterCopy(
		MemoryPlace const *source,
		MemoryPlace const *target,
		DataAccessRegion const &region,
		DataAccess *access
	) {
		assert(target != nullptr);
		assert(access != nullptr);

		ClusterMemoryNode *current = ClusterManager::getCurrentMemoryNode();

		if (source != nullptr
			&& source->getType() != nanos6_cluster_device) {

			assert(source->getType() == nanos6_host_device);
			if (!source->isDirectoryMemoryPlace()) {
				source = current;
			}
		}

		if (target->getType() != nanos6_cluster_device) {
			//! At the moment cluster copies take into account only
			//! Cluster and host devices
			assert(target->getType() == nanos6_host_device);
			assert(!target->isDirectoryMemoryPlace());
			target = current;
		}

		if (target == current) {
			return clusterFetchData(source, target, region, access);
		}

		assert(access->getObjectType() == access_type);
		return new ClusterDataLinkStep(source, target, access);
	}
}


#endif // EXECUTION_WORKFLOW_CLUSTER_HPP
