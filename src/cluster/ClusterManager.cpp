/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#include "ClusterManager.hpp"
#include "messages/MessageSysFinish.hpp"
#include "messages/MessageDataFetch.hpp"

#include "messenger/Messenger.hpp"
#include "polling-services/ClusterServicesPolling.hpp"
#include "polling-services/ClusterServicesTask.hpp"
#include "system/RuntimeInfo.hpp"

#include <NodeNamespace.hpp>
#include <RemoteTasksInfoMap.hpp>
#include <OffloadedTaskId.hpp>
#include <OffloadedTasksInfoMap.hpp>
#include <ClusterNode.hpp>
#include "ClusterUtil.hpp"
#include "WriteID.hpp"
#include "MessageId.hpp"
#include "tasks/Task.hpp"

#include "executors/workflow/cluster/ExecutionWorkflowCluster.hpp"
#include "executors/threads/WorkerThread.hpp"


TaskOffloading::RemoteTasksInfoMap *TaskOffloading::RemoteTasksInfoMap::_singleton = nullptr;
TaskOffloading::OffloadedTasksInfoMap *TaskOffloading::OffloadedTasksInfoMap::_singleton = nullptr;
ClusterManager *ClusterManager::_singleton = nullptr;

std::atomic<size_t> ClusterServicesPolling::_activeClusterPollingServices;
std::atomic<size_t> ClusterServicesTask::_activeClusterTaskServices;

ClusterManager::ClusterManager()
	: _clusterNodes(1),
	_thisNode(new ClusterNode(0, 0)),
	_masterNode(_thisNode),
	_msn(nullptr),
	_disableRemote(false), _disableRemoteConnect(false), _disableAutowait(false)
{
	_clusterNodes[0] = _thisNode;
	WriteIDManager::initialize(0,1);
	OffloadedTaskIdManager::initialize(0,1);
}

ClusterManager::ClusterManager(std::string const &commType, int argc, char **argv)
	: _msn(GenericFactory<std::string,Messenger*,int,char**>::getInstance().create(commType, argc, argv)),
	_disableRemote(false), _disableRemoteConnect(false), _disableAutowait(false)
{
	assert(_msn != nullptr);

	TaskOffloading::RemoteTasksInfoMap::init();
	TaskOffloading::OffloadedTasksInfoMap::init();

	this->internal_reset();

	_msn->synchronizeAll();

	ConfigVariable<bool> inTask("cluster.services_in_task");
	_taskInPoolins = inTask.getValue();

	ConfigVariable<bool> disableRemote("cluster.disable_remote");
	_disableRemote = disableRemote.getValue();

	ConfigVariable<bool> disableRemoteConnect("cluster.disable_remote_connect");
	_disableRemoteConnect = disableRemoteConnect.getValue();

	ConfigVariable<bool> disableAutowait("cluster.disable_autowait");
	_disableAutowait = disableAutowait.getValue();

	ConfigVariable<bool> eagerWeakFetch("cluster.eager_weak_fetch");
	_eagerWeakFetch = eagerWeakFetch.getValue();

	ConfigVariable<bool> eagerSend("cluster.eager_send");
	_eagerSend = eagerSend.getValue();

	ConfigVariable<bool> mergeReleaseAndFinish("cluster.merge_release_and_finish");
	_mergeReleaseAndFinish = mergeReleaseAndFinish.getValue();

	ConfigVariable<int> numMessageHandlerWorkers("cluster.num_message_handler_workers");
	_numMessageHandlerWorkers = numMessageHandlerWorkers.getValue();
}

ClusterManager::~ClusterManager()
{
	for (auto &node : _clusterNodes) {
		delete node;
	}
	_clusterNodes.clear();

	delete _msn;
	_msn = nullptr;
}

// Static 
void ClusterManager::initClusterNamespace(void (*func)(void *), void *args)
{
	assert(_singleton != nullptr);
	NodeNamespace::init(func, args);
}

void ClusterManager::finishClusterNamespace()
{
	assert(_singleton != nullptr);
	do {} while (!NodeNamespace::isEnabled());

	NodeNamespace::notifyShutdown();
	ClusterManager::synchronizeAll();
}



void ClusterManager::internal_reset() {

	/** These are communicator-type indices. At the moment we have an
	 * one-to-one mapping between communicator-type and runtime-type
	 * indices for cluster nodes */

	const size_t clusterSize = _msn->getClusterSize();
	const int nodeIndex = _msn->getNodeIndex();
	const int masterIndex = _msn->getMasterIndex();

	// TODO: Check if this initialization may conflict somehow.
	MessageId::initialize(nodeIndex, clusterSize);
	WriteIDManager::initialize(nodeIndex, clusterSize);
	OffloadedTaskIdManager::initialize(nodeIndex, clusterSize);

	if (this->_clusterNodes.empty()) {
		// Called from constructor the first time
		this->_clusterNodes.resize(clusterSize);

		for (size_t i = 0; i < clusterSize; ++i) {
			_clusterNodes[i] = new ClusterNode(i, i);
		}

		_thisNode = _clusterNodes[nodeIndex];
		_masterNode = _clusterNodes[masterIndex];
	}
}


// Cluster is initialized before the memory allocator.
void ClusterManager::initialize(int argc, char **argv)
{
	assert(_singleton == nullptr);
	ConfigVariable<std::string> commType("cluster.communication");

	RuntimeInfo::addEntry("cluster_communication", "Cluster Communication Implementation", commType);

	/** If a communicator has not been specified through the
	 * cluster.communication config variable we will not
	 * initialize the cluster support of Nanos6 */
	if (commType.getValue() != "disabled") {
		assert(argc > 0);
		assert(argv != nullptr);
		_singleton = new ClusterManager(commType.getValue(), argc, argv);
	} else {
		_singleton = new ClusterManager();
	}

	assert(_singleton != nullptr);
}

// This needs to be called AFTER initializing the memory allocator
void ClusterManager::postinitialize()
{
	assert(_singleton != nullptr);
	assert(MemoryAllocator::isInitialized());

	FatalErrorHandler::failIf(!_singleton->_msn,
							  "This version needs cluster.communication != disabled");

	/* For (verbose) instrumentation, summarize the splitting of external ranks
	 * into appranks and instances. Always do this, even if in non-cluster mode,
	 * as useful for the "per-node" instrumentation of DLB (using num_cores).
	 */
	assert(_singleton->_msn != nullptr);
	_singleton->_msn->summarizeSplit();

	if (inClusterMode()) {

		if (_singleton->_taskInPoolins) {
			ClusterServicesTask::initialize();
			ClusterServicesTask::initializeWorkers(_singleton->_numMessageHandlerWorkers);
		} else {
			ClusterServicesPolling::initialize();
			ClusterServicesTask::initializeWorkers(_singleton->_numMessageHandlerWorkers);
		}
	}
}

void ClusterManager::shutdownPhase1()
{
	assert(NodeNamespace::isEnabled());
	assert(_singleton != nullptr);
	assert(MemoryAllocator::isInitialized());

	if (inClusterMode()) {
		if (_singleton->_taskInPoolins) {
			ClusterServicesTask::waitUntilFinished();
		} else {
			ClusterServicesPolling::waitUntilFinished();
		}
	}

	if (isMasterNode()) {
		MessageSysFinish msg(_singleton->_thisNode);
		sendMessageToAll(&msg, true);

		// Master needs to do the same than others
		ClusterManager::finishClusterNamespace();
	}

	if (inClusterMode()) {
		if (_singleton->_taskInPoolins) {
			ClusterServicesTask::shutdown();
		} else {
			ClusterServicesPolling::shutdown();
		}
		ClusterServicesTask::shutdownWorkers(_singleton->_numMessageHandlerWorkers);

		assert(ClusterServicesPolling::_activeClusterPollingServices == 0);

		TaskOffloading::RemoteTasksInfoMap::shutdown();
		TaskOffloading::OffloadedTasksInfoMap::shutdown();
	}

	if (_singleton->_msn != nullptr) {
		// Finalize MPI BEFORE the instrumentation because the extrae finalization accesses to some
		// data structures throw extrae_nanos6_get_thread_id when finalizing MPI.
		_singleton->_msn->shutdown();
	}
}

void ClusterManager::shutdownPhase2()
{
	// To avoid some issues with the instrumentation shutdown this must be called after finalizing
	// the instrumentation. The extrae instrumentation accesses to the taskInfo->implementations[0]
	// during finalization so if the taskinfo is deleted the access may be corrupt.
	NodeNamespace::deallocate();

	assert(!NodeNamespace::isEnabled());
	assert(_singleton != nullptr);

	delete _singleton;
	_singleton = nullptr;
}

void ClusterManager::fetchVector(
	size_t nFragments,
	std::vector<ExecutionWorkflow::ClusterDataCopyStep *> const &copySteps,
	MemoryPlace const *from
) {
	assert(_singleton->_msn != nullptr);
	assert(from != nullptr);
	assert(from->getType() == nanos6_cluster_device);
	assert((size_t)from->getIndex() < _singleton->_clusterNodes.size());

	ClusterNode const *remoteNode = getClusterNode(from->getIndex());

	assert(remoteNode != _singleton->_thisNode);

	//! At the moment we do not translate addresses on remote
	//! nodes, so the region we are fetching, on the remote node is
	//! the same as the local one
	MessageDataFetch *msg = new MessageDataFetch(_singleton->_thisNode, nFragments, copySteps);

	__attribute__((unused)) MessageDataFetch::DataFetchMessageContent *content = msg->getContent();

	size_t index = 0;

	std::vector<DataTransfer *> temporal(nFragments, nullptr);

	for (ExecutionWorkflow::ClusterDataCopyStep const *step : copySteps) {

		const std::vector<ExecutionWorkflow::FragmentInfo> &fragments = step->getFragments();

		for (__attribute__((unused)) ExecutionWorkflow::FragmentInfo const &fragment : fragments) {
			assert(index < nFragments);
			assert(content->_remoteRegionInfo[index]._remoteRegion == fragment._region);
			temporal[index] = fragment._dataTransfer;

			++index;
		}
	}

	assert(index == nFragments);

	ClusterPollingServices::PendingQueue<DataTransfer>::addPendingVector(temporal);

	_singleton->_msn->sendMessage(msg, remoteNode);
}

void ClusterManager::setEarlyRelease(nanos6_early_release_t early_release)
{
	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	Task *task = currentThread->getTask();
	assert(task != nullptr);
	switch(early_release) {
		case nanos6_no_wait:
			task->setDelayedRelease(false);
			break;

		case nanos6_autowait:
			task->setDelayedNonLocalRelease();
			break;

		case nanos6_wait:
			task->setDelayedRelease(true);
			break;
	}
}
