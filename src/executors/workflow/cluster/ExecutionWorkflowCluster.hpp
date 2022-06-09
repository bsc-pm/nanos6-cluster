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
#include <InstrumentCluster.hpp>

#include <MessageReleaseAccess.hpp>
#include <MessageDataSend.hpp>

#include "executors/threads/WorkerThread.hpp"
#include "dependencies/DataAccessType.hpp"

class ComputePlace;
class MemoryPlace;
class DataTransfer;

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

		//! true if the access is weak
		bool _weak;

		//! type of the corresponding access 
		DataAccessType _accessType;

		OffloadedTaskIdManager::OffloadedTaskId _namespacePredecessor;
		int _namespacePredecessorNode;
		WriteID _writeID;

		bool _started;

		bool _allowEagerSend;

	public:
		ClusterDataLinkStep(
			MemoryPlace const *sourceMemoryPlace,
			MemoryPlace const *targetMemoryPlace,
			DataAccess *access,
			CPUDependencyData &hpDependencyData
		) : DataLinkStep(access),
			_sourceMemoryPlace(sourceMemoryPlace),
			_targetMemoryPlace(targetMemoryPlace),
			_region(access->getAccessRegion()),
			_task(access->getOriginator()),
			_read(access->readSatisfied()),
			_write(access->writeSatisfied()),
			_weak(access->isWeak()),
			_accessType(access->getType()),
			_namespacePredecessor(OffloadedTaskIdManager::InvalidOffloadedTaskId),
			_namespacePredecessorNode(VALID_NAMESPACE_UNKNOWN),
			_writeID((access->getType() == COMMUTATIVE_ACCESS_TYPE) ? 0 : access->getWriteID()),
			_started(false),
			// Eager send is not compatible with weakconcurrent accesses, because
			// an updated location is used to mean that the data was updated by
			// a (strong) concurrent access.
			_allowEagerSend(access->getType() != CONCURRENT_ACCESS_TYPE && access->getType() != WRITE_ACCESS_TYPE)
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
			} else if (access->getType() == COMMUTATIVE_ACCESS_TYPE
						|| access->getType() == CONCURRENT_ACCESS_TYPE) {
				// A satisfied commutative or concurrent access is sent with pseudowrite and pseudoread
				if (access->satisfied()) {
					_write = true;
					_read = true;
				}
			}

			assert(targetMemoryPlace->getType() == nanos6_device_t::nanos6_cluster_device);

			// We do not support weakcommutative accesses on offloaded tasks.  Since
			// the scoreboard is local to each node, we have to treat a weakcommutative
			// access as a strong one if the task is offloaded. We don't know whether it
			// is will actually be offloaded until it is scheduled, so we need to be
			// conservative (do so for all potentially offloadable tasks). But the runtime
			// does not seem to support nested strong commutative accesses, which could
			// happen in two ways: (1) a task with a weakcommutative access (which becomes
			// strong) is not offloaded in the end and (2) it is offloaded but then a
			// strong subtask (or subsubtask, etc.) is offloaded back to the original node.
			// There is a single scoreboard per node, which does not support a potentially
			// arbitrary number of nesting levels.
			FatalErrorHandler::failIf(access->getType() == COMMUTATIVE_ACCESS_TYPE
								      && access->isWeak()
									  && targetMemoryPlace != ClusterManager::getCurrentMemoryNode(),
									  "weakcommutative accesses are not supported for offloaded tasks");

			const int targetNamespace = targetMemoryPlace->getIndex();

			/* Starting workflow on another node: set the namespace and predecessor task */
			if (ClusterManager::getDisableRemote()) {
				_namespacePredecessor = OffloadedTaskIdManager::InvalidOffloadedTaskId;
				_namespacePredecessorNode = VALID_NAMESPACE_NONE;
			} else {
				if (access->getValidNamespacePrevious() == targetNamespace) {
					assert(access->getType() != COMMUTATIVE_ACCESS_TYPE);
				}
				_namespacePredecessor = access->getNamespacePredecessor(); // remote propagation valid if predecessor task and offloading node matches
				_namespacePredecessorNode = access->getValidNamespacePrevious();
			}

			DataAccessRegistration::setNamespaceSelf(access, targetNamespace, hpDependencyData);
		}

		void linkRegion(
			DataAccess const *region,
			bool read,
			bool write,
			TaskOffloading::SatisfiabilityInfoMap &satisfiabilityMap,
			TaskOffloading::DataSendRegionInfoMap &dataSendRegionInfoMap) override;

		//! Start the execution of the Step
		void start() override;
	};

	struct FragmentInfo {
		DataAccessRegion _region;
		int _id;
		DataTransfer *_dataTransfer;
	};

	class ClusterDataCopyStep : public Step {
		//! The MemoryPlace that the data will be copied from.
		MemoryPlace const * const _sourceMemoryPlace;

		//! The MemoryPlace that the data will be copied to.
		MemoryPlace const * const _targetMemoryPlace;

		//! A mapping of the address range in the source node to the target node.
		DataAccessRegion const _fullRegion;
		std::vector<FragmentInfo> _regionsFragments;

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

		DataTransfer::transfer_callback_t _postcallback;

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
			return 1;
		}

		const std::vector<FragmentInfo> &getFragments() const
		{
			return _regionsFragments;
		}

		DataTransfer::transfer_callback_t getPostCallback() const
		{
			return _postcallback;
		}
	};

	class ClusterDataReleaseStep : public DataReleaseStep {
		//! identifier of the remote task
		OffloadedTaskIdManager::OffloadedTaskId _remoteTaskIdentifier;

		//! the cluster node we need to notify
		ClusterNode const *_offloader;

		MessageReleaseAccess::ReleaseAccessInfoVector _releaseInfo;

		MessageReleaseAccess *releaseInfoVectorToMessage(bool releaseTask)
		{
#ifndef NDEBUG
			assert(_releaseInfo.size() > 0 || releaseTask);
#endif // NDEBUG

			MessageReleaseAccess *msg = new MessageReleaseAccess(
				ClusterManager::getCurrentClusterNode(),
				_remoteTaskIdentifier,
				releaseTask,
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
			_task->setDataReleaseStep(this);
		}

		virtual inline ~ClusterDataReleaseStep()
		{
			_task->unsetDataReleaseStep();
			assert(_releaseInfo.size() == 0);
		}

		void addAccess(DataAccess *access)
		{
			_bytesToRelease.fetch_add(access->getAccessRegion().getSize());
		}

		void releasePendingAccesses(bool releaseTask) override
		{
			_infoLock.lock();
			// check atomically without taking the lock in case we can return immediately faster.
			if (_releaseInfo.size() == 0 && releaseTask == false) {
				_infoLock.unlock();
				return;
			}

			MessageReleaseAccess *msg = releaseInfoVectorToMessage(releaseTask);

			if (!releaseTask) {
				// When we are not going to finalize the task we release the lock before calling
				// mpi. So any other thread in the dependency system can continue storing things in
				// the container.
				_infoLock.unlock();
				ClusterManager::sendMessage(msg, _offloader);
			} else {
				// When we are going to finalize the task then we hold the lock because it is
				// not expected that other thread add any release region anymore.

				// TODO: When fixed local propagation discount in _bytesToRelease we can enable this
				// assert. Deleting this fixed the memory leak issue we had before where the release
				// step was not always deleted.
				// assert(_bytesToRelease.load() == 0);
				ClusterManager::sendMessage(msg, _offloader);
				_infoLock.unlock();

				delete this;
			}
		}

		void addToReleaseList(DataAccess const *access) override
		{
			assert(access->getLocation() != nullptr);

			DataAccessRegion const &region = access->getAccessRegion();
			MemoryPlace const *location = access->getLocation();

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

			_infoLock.lock();

			_releaseInfo.push_back(
				MessageReleaseAccess::ReleaseAccessInfo(
					region,
					access->getWriteID(),
					clusterLocation)
			);

			_infoLock.unlock();
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
				&& (!access->hasNext()                      // no next access at the remote side or propagating to an "in" access
					|| access->getNamespaceNextIsIn())
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
		TaskOffloading::SatisfiabilityInfoVector _satInfo;
		TaskOffloading::DataSendRegionInfoVector _dataSendRegionInfo;
		ClusterNode *_remoteNode;
		Task *_task;

	public:
		ClusterExecutionStep(Task *task, ComputePlace *computePlace)
			: Step(),
			_satInfo(),
			_dataSendRegionInfo(),
			_remoteNode(dynamic_cast<ClusterNode *>(computePlace)),
			_task(task)
		{
			assert(computePlace->getType() == nanos6_cluster_device);
			assert(_remoteNode != nullptr);   /// the dynamic_cast worked

			TaskOffloading::ClusterTaskContext *clusterContext =
				new TaskOffloading::ClusterTaskContext(_task, task->getOffloadedTaskId(), _remoteNode);
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
		//! \param[in] weak is true if access is weak
		//! \param[in] accessType type of the access
		//! \param[in] namespacePredecessorId is nullptr or predecessor remote task ID
		void addDataLink(
			int source,
			DataAccessRegion const &region,
			WriteID writeID,
			bool read, bool write,
			bool weak, DataAccessType accessType,
			OffloadedTaskIdManager::OffloadedTaskId namespacePredecessorId,
			int eagerWeakSendTag
		) {
			// This lock should already have been taken by the caller
			// Apparently it is not.
			//assert(_lock.isLockedByThisThread());

			// Satisfiability info to send to target
			_satInfo.push_back(
				TaskOffloading::SatisfiabilityInfo(
					region, source,
					read, write,
					weak, accessType,
					writeID, namespacePredecessorId, eagerWeakSendTag)
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
		DataAccess *access,
		CPUDependencyData &hpDependencyData
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
		if (source->isClusterLocalMemoryPlace()) {
			// NULL copy (do nothing, just release succesor and delete itself.)
			return new Step();
		}

		if (WriteIDManager::checkWriteIDLocal(access->getWriteID(), region)) {
			// It is present locally, even though the location for this particular
			// data access says otherwise. Update the location to reflect the fact that the
			// data is local, otherwise disableReadPropagationToNext will not pass read
			// satisfiability to the successor until the access is complete. Note: this
			// may create delayed operations (passing the read satisfiability, which is
			// why this function needs hpDependencyData).
			Instrument::dataFetch(Instrument::EarlyWriteID, region);
			if (!access->isWeak() && access->getType() != READ_ACCESS_TYPE) {
				access->setNewLocalWriteID();
			}
			if (access->readSatisfied()) {
				DataAccessRegistration::setLocationFromWorkflow(access, ClusterManager::getCurrentMemoryNode(), hpDependencyData);
			}
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
			if (ClusterManager::getEagerWeakFetch() || ClusterManager::getEagerSend()) {

				FatalErrorHandler::fail(
					"Set cluster.eager_send = false and cluster.eager_weak_fetch = false for large weak memory access ",
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
				//! and, if it is a weak access, then only if cluster.eager_weak_fetch == true.
				//! Also don't eagerly fetch weak concurrent accesses, as usually we will
				//! only access part of them.
				&& (!isWeak || (ClusterManager::getEagerWeakFetch() && access->getType() != CONCURRENT_ACCESS_TYPE))
			);

		//! If no data transfer is needed, then register the new location if
		//! it is a task with a non-weak access. This happens for out dependencies
		//! (WRITE_ACCESS_TYPE), because the task will write the new data contents.
		//! It also happens when the data was previously uninitialized (was in
		//! the directory) even on an in or inout dependency.
		bool registerLocation = !needsTransfer
			&& objectType != taskwait_type
			&& !isWeak;

		//! If it is a taskwait that doesn't need a transfer, then clear the
		//! output location to tell handleExitTaskwait that it hasn't been copied
		//! here.
		if (objectType == taskwait_type && !needsTransfer) {
			access->setOutputLocation(nullptr);
		}

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
		DataAccess *access,
		CPUDependencyData &hpDependencyData
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
			return clusterFetchData(source, target, region, access, hpDependencyData);
		}

		assert(access->getObjectType() == access_type);
		return new ClusterDataLinkStep(source, target, access, hpDependencyData);
	}
}


#endif // EXECUTION_WORKFLOW_CLUSTER_HPP
