/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#include "ClusterManager.hpp"
#include "messages/MessageSysFinish.hpp"
#include "messenger/Messenger.hpp"
#include "polling-services/ClusterPollingServices.hpp"
#include "support/config/ConfigVariable.hpp"
#include "system/RuntimeInfo.hpp"

#include <ClusterNode.hpp>

ClusterManager *ClusterManager::_singleton = nullptr;

ClusterManager::ClusterManager() :
	_clusterNodes(1),
	_thisNode(new ClusterNode(0, 0)),
	_masterNode(_thisNode),
	_msn(nullptr)
{
	_clusterNodes[0] = _thisNode;
}

ClusterManager::ClusterManager(std::string const &commType)
{
	_msn = GenericFactory<std::string, Messenger *>::getInstance().create(commType);
	assert(_msn);

	/** These are communicator-type indices. At the moment we have an
	 * one-to-one mapping between communicator-type and runtime-type
	 * indices for cluster nodes */
	const size_t clusterSize = _msn->getClusterSize();
	const int nodeIndex = _msn->getNodeIndex();
	const int masterIndex = _msn->getMasterIndex();

	_clusterNodes.resize(clusterSize);

	for (size_t i = 0; i < clusterSize; ++i) {
		_clusterNodes[i] = new ClusterNode(i, i);
	}

	_thisNode = _clusterNodes[nodeIndex];
	_masterNode = _clusterNodes[masterIndex];

	if (clusterSize > 1) {
		ClusterPollingServices::initialize();
	}

	_msn->synchronizeAll();
	_callback.store(nullptr);
}

ClusterManager::~ClusterManager()
{
	for (auto &node : _clusterNodes) {
		delete node;
	}

	if (inClusterMode()) {
		ClusterPollingServices::shutdown();
	}

	delete _msn;
}

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
		return;
	}

	_singleton = new ClusterManager();
	assert(_singleton != nullptr);
}

void ClusterManager::notifyShutdown()
{
	assert(_singleton != nullptr);

	if (_singleton->isMasterNode() && _singleton->inClusterMode()) {
		for (ClusterNode *slaveNode : _singleton->_clusterNodes) {
			if (slaveNode != _singleton->_thisNode) {
				MessageSysFinish msg(_singleton->_thisNode);
				_singleton->_msn->sendMessage(&msg, slaveNode, true);
			}
		}

		_singleton->_msn->synchronizeAll();
	}
}

void ClusterManager::shutdown()
{
	assert(_singleton != nullptr);
	delete _singleton;
	_singleton = nullptr;
}
