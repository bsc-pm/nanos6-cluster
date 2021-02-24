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

#include <RemoteTasksInfoMap.hpp>
#include <ClusterNode.hpp>
#include <NodeNamespace.hpp>
#include "ClusterUtil.hpp"
#include "WriteID.hpp"
#include "MessageId.hpp"

#include "executors/workflow/cluster/ExecutionWorkflowCluster.hpp"


TaskOffloading::RemoteTasksInfoMap *TaskOffloading::RemoteTasksInfoMap::_singleton = nullptr;
ClusterManager *ClusterManager::_singleton = nullptr;

std::atomic<size_t> ClusterServicesPolling::_activeClusterPollingServices;
std::atomic<size_t> ClusterServicesTask::_activeClusterTaskServices;

ClusterManager::ClusterManager()
	: _clusterNodes(1),
	_thisNode(new ClusterNode(0, 0)),
	_masterNode(_thisNode),
	_msn(nullptr), _usingNamespace(false), _disableRemote(false), _disableAutowait(false),
	_callback(nullptr)
{
	_clusterNodes[0] = _thisNode;
	WriteIDManager::initialize(0,1);
}

ClusterManager::ClusterManager(std::string const &commType)
	:_msn(nullptr), _disableRemote(false), _disableAutowait(false), _callback(nullptr)
{
	TaskOffloading::RemoteTasksInfoMap::init();

	_msn = GenericFactory<std::string, Messenger*>::getInstance().create(commType);
	assert(_msn);

	/** These are communicator-type indices. At the moment we have an
	 * one-to-one mapping between communicator-type and runtime-type
	 * indices for cluster nodes */
	const size_t clusterSize = _msn->getClusterSize();
	const int nodeIndex = _msn->getNodeIndex();
	const int masterIndex = _msn->getMasterIndex();

	MessageId::initialize(nodeIndex, clusterSize);
	WriteIDManager::initialize(nodeIndex, clusterSize);

	_clusterNodes.resize(clusterSize);

	for (size_t i = 0; i < clusterSize; ++i) {
		_clusterNodes[i] = new ClusterNode(i, i);
	}

	_thisNode = _clusterNodes[nodeIndex];
	_masterNode = _clusterNodes[masterIndex];

	_msn->synchronizeAll();
	_callback.store(nullptr);

	ConfigVariable<bool> inTask("cluster.services_in_task");
	_taskInPoolins = inTask.getValue();

	ConfigVariable<bool> usingNamespace("cluster.use_namespace");
	_usingNamespace = usingNamespace.getValue();

	if (_usingNamespace) {
		ConfigVariable<bool> disableRemote("cluster.disable_remote");
		_disableRemote = disableRemote.getValue();
	}

	ConfigVariable<bool> disableAutowait("cluster.disable_autowait");
	_disableAutowait = disableAutowait.getValue();

	ConfigVariable<size_t> messageMaxSize("cluster.message_max_size");
	_messageMaxSize = messageMaxSize.getValue();
}

ClusterManager::~ClusterManager()
{
	for (auto &node : _clusterNodes) {
		delete node;
	}

	delete _msn;

	delete _callback;
}

// Cluster is initialized before the memory allocator.
void ClusterManager::initialize()
{

	assert(_singleton == nullptr);
	ConfigVariable<std::string> commType("cluster.communication");

	RuntimeInfo::addEntry("cluster_communication", "Cluster Communication Implementation", commType);

	/** If a communicator has not been specified through the
	 * cluster.communication config variable we will not
	 * initialize the cluster support of Nanos6 */
	if (commType.getValue() != "disabled") {
		_singleton = new ClusterManager(commType.getValue());
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

	if (inClusterMode()) {

		if (_singleton->_taskInPoolins) {
			ClusterServicesTask::initialize();
		} else {
			ClusterServicesPolling::initialize();
		}
	}

}


void ClusterManager::initClusterNamespaceOrSetCallback(
	void (*func)(void *),
	void *args
) {
	assert(_singleton != nullptr);

	if (_singleton->_usingNamespace) {
		NodeNamespace::init(func, args);
	} else {
		assert(_singleton->_callback.load() == nullptr);
		_singleton->_callback.store(new ClusterShutdownCallback(func, args));
	}
}


void ClusterManager::shutdownPhase1()
{
	assert(_singleton != nullptr);
	assert(MemoryAllocator::isInitialized());

	if (_singleton->_usingNamespace && isMasterNode()) {
		// _usingNamespace duplicates the information of NodeNamespace::isEnabled().
		assert(NodeNamespace::isEnabled());
		NodeNamespace::notifyShutdown();
	}

	if (inClusterMode()) {

		if (isMasterNode()) {
			for (ClusterNode *slaveNode : _singleton->_clusterNodes) {
				if (slaveNode != _singleton->_thisNode) {
					MessageSysFinish msg(_singleton->_thisNode);
					_singleton->_msn->sendMessage(&msg, slaveNode, true);
				}
			}

			_singleton->_msn->synchronizeAll();
		}

		if (_singleton->_taskInPoolins) {
			ClusterServicesTask::shutdown();
		} else {
			ClusterServicesPolling::shutdown();
		}
		assert(ClusterServicesPolling::_activeClusterPollingServices == 0);

		if (NodeNamespace::isEnabled()) {
			NodeNamespace::deallocate();
		}
		assert(!NodeNamespace::isEnabled());

		TaskOffloading::RemoteTasksInfoMap::shutdown();
	}
}

void ClusterManager::shutdownPhase2()
{

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

	MessageDataFetch::DataFetchMessageContent *content = msg->getContent();

	size_t index = 0;

	std::vector<DataTransfer *> temporal(nFragments, nullptr);

	for (ClusterDataCopyStep const *step : copySteps) {

		const std::vector<DataAccessRegion> &fragments = step->getFragments();

		for (__attribute__((unused)) DataAccessRegion const &region : fragments) {
			assert(index < nFragments);
			assert(content->_remoteRegionInfo[index]._remoteRegion == region);
			//_content->_remoteRegionInfo[index] = region;
			//_content->_remoteRegionInfo[index]._id
			//	= (index == 0 ? getId() : MessageId::nextMessageId());


			temporal[index] = fetchDataRaw(
				content->_remoteRegionInfo[index]._remoteRegion,
				from,
				content->_remoteRegionInfo[index]._id,
				false);  // block

			temporal[index]->addCompletionCallback(step->getPostCallback());

			++index;
		}
	}

	assert(index == nFragments);

	ClusterPollingServices::PendingQueue<DataTransfer>::addPendingVector(temporal);

	_singleton->_msn->sendMessage(msg, remoteNode);
}
