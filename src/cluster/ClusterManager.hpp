/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_MANAGER_HPP
#define CLUSTER_MANAGER_HPP

#include <atomic>
#include <cassert>
#include <string>
#include <vector>
#include <type_traits>

#include "cluster/messenger/TransferBase.hpp"
#include "cluster/messenger/Messenger.hpp"
#include "cluster/messenger/DataTransfer.hpp"

#include <ClusterNode.hpp>
#include <MessageDataFetch.hpp>
#include <MessageDataSend.hpp>
#include <ClusterShutdownCallback.hpp>
#include "memory/directory/Directory.hpp"

namespace ExecutionWorkflow
{
	class ClusterDataCopyStep;
}

class ClusterMemoryNode;

class ClusterManager {

private:
	bool _clusterRequested; // Built with cluster and cluster.communication is not disabled
	int _totalReadyTasks;

	static ClusterManager *_singleton;

	//! A vector of all ClusterNodes in the system.
	//!
	//! We might need to make this a map later on, when we start
	//! adding/removing nodes
	std::vector<ClusterNode *> _clusterNodes;

	//! ClusterNode object of the current node
	ClusterNode * _thisNode;

	//! ClusterNode of the master node
	ClusterNode * _masterNode;

	//! Messenger object for cluster communication.
	Messenger * _msn;

	//! Using cluster namespace
	bool _disableRemote;
	bool _disableRemoteConnect;

	bool _disableAutowait;

	bool _eagerWeakFetch;

	bool _eagerSend;

	bool _mergeReleaseAndFinish;

	bool _autoOptimizeNonAccessed;

	bool _autoOptimizeReadOnly;

	int _numMessageHandlerWorkers;

	//! private constructors. This is a singleton.
	ClusterManager();

	ClusterManager(std::string const &commType, int argc, char **argv);

	~ClusterManager();

	void internal_reset();

public:
	//! \brief Initialize the ClusterManager
	//! This is called before initializing the memory allocator because it collects some
	//! information needed by the memory allocator latter.
	static void initialize(int argc, char **argv);

	//! \brief postInitialize the ClusterManager
	//! This is called after the memory allocator is already initialized because it starts the
	//! pooling services which are tasks.
	static void postinitialize();

	//! \brief Notify all cluster nodes that we are shutting down
	static void shutdownPhase1();

	//! \brief Shutdown the ClusterManager
	static void shutdownPhase2();

	//! \brief Get a vector containing all ClusterNode objects
	//!
	//! \returns A vector containing all ClusterNode objects
	static inline std::vector<ClusterNode *> const &getClusterNodes()
	{
		assert(_singleton != nullptr);
		assert(!_singleton->_clusterNodes.empty());
		return _singleton->_clusterNodes;
	}

	//! \brief Get the ClusterNode representing the master node
	//!
	//! \returns the master node ClusterNode
	static inline ClusterNode *getMasterNode()
	{
		assert(_singleton != nullptr);
		return _singleton->_masterNode;
	}

	//! \brief Get the ClusterNode with index 'id'
	//!
	//! \param[in] id is the index of the ClusterNode we request
	//!
	//! \returns The ClusterNode object with index 'id'
	static inline ClusterNode *getClusterNode(size_t id)
	{
		assert(_singleton != nullptr);
		assert(!_singleton->_clusterNodes.empty());
		assert((size_t)id < _singleton->_clusterNodes.size());

		return _singleton->_clusterNodes[id];
	}

	//! \brief Get the current ClusterNode
	//!
	//! \returns the ClusterNode object of the current node
	static inline ClusterNode *getCurrentClusterNode()
	{
		assert(_singleton != nullptr);
		assert(_singleton->_thisNode != nullptr);
		return _singleton->_thisNode;
	}

	//! \brief Get The ClusterMemoryNode with index id;
	//!
	//! \param[in] id is the index of the ClusterMemoryNode we request
	//!
	//! \returns The ClusterMemoryNode object with index 'id'
	static inline ClusterMemoryNode *getMemoryNode(int id)
	{
		assert(_singleton != nullptr);
		assert(!_singleton->_clusterNodes.empty());
		assert(_singleton->_clusterNodes[id] != nullptr);
		assert (!Directory::isDirectoryMemoryPlaceIdx(id));
		return _singleton->_clusterNodes[id]->getMemoryNode();
	}

	static inline const MemoryPlace *getMemoryNodeOrDirectory(int id)
	{
		if (Directory::isDirectoryMemoryPlaceIdx(id)) {
			return Directory::getDirectoryMemoryPlace();
		}

		return getMemoryNode(id);
	}

	//! \brief Get the current ClusterMemoryNode
	//!
	//! \returns the ClusterMemoryNode object of the current node
	static inline ClusterMemoryNode *getCurrentMemoryNode()
	{
		assert(_singleton != nullptr);
		assert(_singleton->_thisNode != nullptr);
		return _singleton->_thisNode->getMemoryNode();
	}

	//! \brief Check if current node is the master
	//!
	//! \returns true if the current node is the master
	static inline bool isMasterNode()
	{
		assert(_singleton != nullptr);
		assert(_singleton->_thisNode != nullptr);
		assert(_singleton->_masterNode != nullptr);
		return _singleton->_masterNode == _singleton->_thisNode;
	}

	//! \brief Get the number of cluster nodes
	//!
	//! \returns the number of cluster nodes
	static inline int clusterSize()
	{
		assert(_singleton != nullptr);
		assert(!_singleton->_clusterNodes.empty());
		return _singleton->_clusterNodes.size();
	}

	//! \brief Check if we run in cluster mode
	//!
	//! We run in cluster mode, if we have compiled with cluster support,
	//! we have enabled Cluster at runtime and we run with more than one
	//! Cluster nodes.
	//!
	//! \returns true if we run in cluster mode
	static inline bool inClusterMode()
	{
		assert(_singleton != nullptr);
		assert(!_singleton->_clusterNodes.empty());
		return _singleton->_clusterNodes.size() > 1;
	}

	static inline bool isInitialized()
	{
		return _singleton != nullptr;
	}

	static inline Messenger *getMessenger()
	{
		assert(_singleton != nullptr);
		return _singleton->_msn;
	}

	// This function aborts all only in cluster mode.
	static inline void tryAbortAll()
	{
		if (_singleton != nullptr && inClusterMode()) {
			_singleton->_msn->abortAll(EXIT_FAILURE);
		}
	}

	//! \brief Check if OmpSs-2@Cluster was requested
	//!
	//! \returns True if built with Cluster support and
	//! cluster.communication is not "disabled".
	static inline bool clusterRequested()
	{
		assert(_singleton);
		return _singleton->_clusterRequested;
	}

	//! \brief Check for incoming messages
	//!
	//! This is just a wrapper on top of the Messenger API
	//!
	//! \returns a Message object if one has been received otherwise,
	//!		nullptr
	static inline Message *checkMail()
	{
		assert(_singleton != nullptr);
		assert(_singleton->_msn != nullptr);
		return _singleton->_msn->checkMail();
	}

	//! \brief Send a Message to a remote Node
	//!
	//! This is just a wrapper on top of the Messenger API
	//!
	//! \param[in] msg is the Message to send
	//! \param[in] recipient is the remote node to send the Message
	//! \param[in] if block is true the the call will block until the
	//!		Message is sent
	static inline void sendMessage(Message *msg, ClusterNode const *recipient, bool block = false)
	{
		assert(_singleton != nullptr);
		assert(_singleton->_msn != nullptr);
		assert(msg != nullptr);
		assert(recipient != nullptr);
		_singleton->_msn->sendMessage(msg, recipient, block);
	}

	static inline void sendMessageToAll(Message *msg, bool block = false)
	{
		assert(_singleton != nullptr);
		assert(_singleton->_msn != nullptr);
		assert(msg != nullptr);

		for (ClusterNode *node : _singleton->_clusterNodes) {
			// Not send to myself and avoid ping-pong.
			if (node == _singleton->_thisNode || msg->getSenderId() == node->getIndex()) {
				continue;
			}

			_singleton->_msn->sendMessage(msg, node, block);
		}
	}

	//! \brief Test Messages for completion
	//!
	//! This is just a wrapper on top of the Messenger API
	//!
	//! \param[in] messages is a vector containing Message objects
	//!		to check for completion
	template<typename T>
	static inline void testCompletion(std::vector<T *> &messages)
	{
		assert(_singleton != nullptr);
		assert(_singleton->_msn != nullptr);
		static_assert(std::is_base_of<TransferBase,T>::value, "messages must be subclasses of TransferBase");

		_singleton->_msn->testCompletion(messages);
	}

	template<typename T>
	static inline void waitAllCompletion(std::vector<T *> &messages)
	{
		assert(_singleton != nullptr);
		assert(_singleton->_msn != nullptr);
		static_assert(std::is_base_of<TransferBase,T>::value, "messages must be subclasses of TransferBase");

		if (messages.size() == 0) {
			return;
		}

		std::vector<TransferBase *> &tmp = reinterpret_cast<std::vector<TransferBase *>&>(messages);

		_singleton->_msn->waitAllCompletion(tmp);
	}


	//! \brief Fetch a DataAccessRegion from a remote node
	//!
	//! \param[in] region is the address region to fetch
	//! \param[in] from is the remote MemoryPlace we are fetching from
	//! \param[in] messageId is the index of the Message with which this
	//!		DataTransfer is related
	//! \param[in] if block is true the call will block until the data is
	//!		received
	//!
	//! \returns a DataTransfer object if data was received non-blocking,
	//!		otherwise nullptr
	static inline DataTransfer *fetchDataRaw(
		DataAccessRegion const &region,
		MemoryPlace const *from,
		int messageId,
		bool block = false,
		bool instrument = true
	) {
		assert(_singleton != nullptr);
		assert(_singleton->_msn != nullptr);
		assert(from != nullptr);

		ClusterNode const *remoteNode = getClusterNode(from->getIndex());
		return _singleton->_msn->fetchData(region, remoteNode, messageId, block, instrument);
	}

	//! \brief Send a DataAccessRegion to a remote node
	//!
	//! \param[in] region is the address region to send
	//! \param[in] to is the remote MemoryPlace we are sending to
	//! \param[in] messageId is the index of the Message with which this
	//!		DataTransfer is related
	//! \param[in] if block is true the call will block until the data is
	//!		sent
	//!
	//! \returns a DataTransfer object if data was sent non-blocking,
	//!		otherwise nullptr
	static inline DataTransfer *sendDataRaw(
		DataAccessRegion const &region,
		MemoryPlace const *to,
		int messageId,
		bool block = false,
		bool instrument = true
	) {
		assert(_singleton != nullptr);
		assert(_singleton->_msn != nullptr);
		assert(to != nullptr);
		assert(to->getType() == nanos6_cluster_device);
		assert(to->getIndex() != _singleton->_thisNode->getIndex());

		ClusterNode const *remoteNode = getClusterNode(to->getIndex());
		return _singleton->_msn->sendData(region, remoteNode, messageId, block, instrument);
	}

	//! \brief Initiate a data fetch operation
	//!
	//! \param[in] region is the local region we want to update with data
	//!		from the remote node
	//! \param[in] from is the MemoryPlace we fetch the data from. This
	//!		must be a cluster memory place
	//! \param[in] postcallback is a function that will be set as a callback for the
	//!     datatransfer associated with fetchDataRaw callback.
	//! \param[in] block determines whether the operation will be blocking.
	//!		If block is true then upon return, the data will have
	//!		been succesfully fetched and region will be updated.
	//!
	//! \returns In non-blocking mode, this method returns a DataTransfer
	//!		object which can be used to track the completion of the
	//!		data transfer. In blocking mode this always returns
	//!		nullptr
	static void fetchVector(
		size_t nFragments,
		std::vector<ExecutionWorkflow::ClusterDataCopyStep *> const &copySteps,
		MemoryPlace const *from
	);

	//! \brief A barrier across all cluster nodes
	//!
	//! This is a collective operation. It needs to be invoked by all
	//! cluster nodes, otherwise a deadlock will occur. Execution of the
	//! cluster node will be blocked until all nodes reach at the matching
	//! synchronization point.
	static inline void synchronizeAll()
	{
		assert(_singleton != nullptr);
		if (inClusterMode()) {
			assert(_singleton->_msn != nullptr);
			_singleton->_msn->synchronizeAll();
		}
	}

	//! \brief Functions required to init and finish.
	//!
	//! Init is called in the boostrap and finish in the SysFInish Message handler.
	//! invoked when we have to shutdown the runtime instance
	static void initClusterNamespace(void (*func)(void *), void *args);

	static bool getDisableRemote()
	{
		assert(_singleton != nullptr);
		return _singleton->_disableRemote;
	}

	static bool getDisableRemoteConnect()
	{
		assert(_singleton != nullptr);
		return _singleton->_disableRemoteConnect;
	}

	static bool getDisableAutowait()
	{
		assert(_singleton != nullptr);
		return _singleton->_disableAutowait;
	}

	static size_t getMPIFragments(DataAccessRegion const &remoteRegion)
	{
		const size_t totalSize = remoteRegion.getSize();
		assert(totalSize > 0);

		const size_t maxRegionSize = _singleton->_msn->getMessageMaxSize();
		// Note: this calculation still works when maxRegionSize == SIZE_MAX.
		size_t nFragments = totalSize / maxRegionSize;
		if ((totalSize % maxRegionSize) != 0) {
			nFragments++;
		}
		return nFragments;
	}

	static bool getEagerWeakFetch()
	{
		assert(_singleton != nullptr);
		return _singleton->_eagerWeakFetch;
	}

	static bool getEagerSend()
	{
		assert(_singleton != nullptr);
		return _singleton->_eagerSend;
	}

	static bool getMergeReleaseAndFinish()
	{
		assert(_singleton != nullptr);
		return _singleton->_mergeReleaseAndFinish;
	}

	static bool autoOptimizeNonAccessed()
	{
		assert(_singleton != nullptr);
		return _singleton->_autoOptimizeNonAccessed;
	}

	static bool autoOptimizeReadOnly()
	{
		assert(_singleton != nullptr);
		return _singleton->_autoOptimizeReadOnly;
	}

	static int getNumMessageHandlerWorkers()
	{
		assert(_singleton != nullptr);
		return _singleton->_numMessageHandlerWorkers;
	}

	//! \brief Get the application communicator
	//!
	//! \returns the application communicator
	static inline int getAppCommunicator()
	{
		assert(_singleton->_msn != nullptr);
		return _singleton->_msn->getAppCommunicator();
	}

	//! \brief Get the apprank number
	//!
	//! \returns the apprank number
	static inline int getApprankNum()
	{
		assert(_singleton);
		assert(_singleton->_msn != nullptr);
		return _singleton->_msn->getApprankNum();
	}

	//! \brief Get the physical node number
	//!
	//! \returns the physical node number (within the job)
	static inline int getPhysicalNodeNum()
	{
		assert(_singleton);
		assert(_singleton->_msn != nullptr);
		return _singleton->_msn->getPhysicalNodeNum();
	}

	//! \brief Get the external rank
	//!
	//! \returns the external rank (in MPI_COMM_WORLD)
	static inline int getExternalRank()
	{
		assert(_singleton);
		assert(_singleton->_msn != nullptr);
		return _singleton->_msn->getExternalRank();
	}

	//! \brief Get the number of external ranks
	//!
	//! \returns the number of external ranks (in MPI_COMM_WORLD)
	static inline int getNumExternalRanks()
	{
		assert(_singleton);
		assert(_singleton->_msn != nullptr);
		return _singleton->_msn->getNumExternalRanks();
	}

	//! \brief Get the index number of the instance on the
	//! current physical node
	//!
	//! \returns The index number of this instance on the
	//! physical node.
	static inline int getIndexThisPhysicalNode()
	{
		assert(_singleton);
		assert(_singleton->_msn != nullptr);
		return _singleton->_msn->getIndexThisPhysicalNode();
	}

	// For now just a bool to indicate whether each instance is a master;
	// in future it may have to return more information.
	static const std::vector<bool> &getIsMasterThisNode()
	{
		assert(_singleton);
		assert(_singleton->_msn);
		return _singleton->_msn->getIsMasterThisNode();
	}

	static int getExternalRankThisNode(int indexThisPhysicalNode)
	{
		assert(_singleton);
		assert(_singleton->_msn != nullptr);
		return _singleton->_msn->getInstanceThisNodeToExternalRank()[indexThisPhysicalNode];
	}

	static void setTotalReadyTasks(int totalReadyTasks)
	{
		assert(_singleton);
		_singleton->_totalReadyTasks = totalReadyTasks;
		Instrument::emitClusterEvent(Instrument::ClusterEventType::TotalApprankReadyTasks, totalReadyTasks);
	}

	static int getTotalReadyTasks()
	{
		assert(_singleton);
		return _singleton->_totalReadyTasks;
	}

};




#endif /* CLUSTER_MANAGER_HPP */
