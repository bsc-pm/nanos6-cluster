/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_MANAGER_HPP
#define CLUSTER_MANAGER_HPP

#include <string>
#include <vector>

#include <ClusterMemoryNode.hpp>
#include <ClusterNode.hpp>
#include "DataAccessRegion.hpp"

class Message;
class DataTransfer;

class ClusterManager {
	//! private constructor. This is a singleton.
	ClusterManager()
	{}
public:
	class ShutdownCallback
	{
	};

	static inline void initialize()
	{
		// WriteIDManager::initialize(0, 1);
	}

	static inline void postinitialize()
	{
	}

	static inline void shutdownPhase1()
	{
	}

	static inline void shutdownPhase2()
	{
	}

	static inline ClusterNode *getCurrentClusterNode()
	{
		static ClusterNode ourDummyNode;
		return &ourDummyNode;
	}

	static inline ClusterMemoryNode *getMemoryNode(__attribute__((unused)) int id)
	{
		static ClusterMemoryNode ourDummyNode;
		return &ourDummyNode;
	}

	static inline ClusterMemoryNode *getCurrentMemoryNode()
	{
		static ClusterMemoryNode ourDummyNode;
		return &ourDummyNode;
	}

	static inline bool isMasterNode()
	{
		return true;
	}

	static inline int clusterSize()
	{
		return 1;
	}

	static inline bool inClusterMode()
	{
		return false;
	}

	static inline Message *checkMail()
	{
		return nullptr;
	}

	static inline void testMessageCompletion(__attribute__((unused)) std::vector<Message *> &messages) {
	}

	static inline void testDataTransferCompletion(__attribute__((unused)) std::vector<Message *> &transfer) {
	}

	static inline DataTransfer *fetchDataRaw(
		__attribute__((unused)) DataAccessRegion const &region,
		__attribute__((unused)) MemoryPlace const *from,
		__attribute__((unused)) int messageId,
		__attribute__((unused)) bool block
	) {
		return nullptr;
	}

	static inline DataTransfer *sendDataRaw(
		__attribute__((unused)) DataAccessRegion const &region,
		__attribute__((unused)) MemoryPlace const *to,
		__attribute__((unused)) int messageId,
		__attribute__((unused)) bool block
	) {
		return nullptr;
	}

	static inline void initClusterNamespaceOrSetCallback(
		__attribute__((unused)) void (*func)(void *),
		__attribute__((unused)) void *args)
	{
	}

	static inline bool getDisableRemote()
	{
		return true;
	}

	static inline bool isLocalMemoryPlace(const __attribute__((unused)) MemoryPlace *location)
	{
		return true;
	}

	static inline void synchronizeAll()
	{
	}
};

#endif /* CLUSTER_MANAGER_HPP */
