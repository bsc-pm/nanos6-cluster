/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_SERVICES_POLLING_HPP
#define CLUSTER_SERVICES_POLLING_HPP

#include "MessageHandler.hpp"
#include "PendingQueue.hpp"

#if HAVE_DLB
#include "hybrid/HybridPolling.hpp"
#endif

class ClusterServicesPolling {

	// Defined in ClusterManager.cpp
	static std::atomic<size_t> _activeClusterPollingServices;
	static std::atomic<bool> _pausedServices;

	template <typename T>
	static int bodyClusterService(__attribute__((unused)) void *args)
	{
		if (!_pausedServices.load()) {
			T::executeService(); // This returns true unless the service is unregistered
		}
		return 0;
	}

	template<typename T>
	static void registerService(const std::string &name)
	{
		const std::string label = "ClusterPolling_" + name;
		_activeClusterPollingServices.fetch_add(1);

		T::registerService();

		nanos6_register_polling_service(label.c_str(), bodyClusterService<T>, nullptr);
	}

	template<typename T>
	static void unregisterService(const std::string &name)
	{
		const std::string label = "ClusterPolling_" + name;
		nanos6_unregister_polling_service(label.c_str(), bodyClusterService<T>, nullptr);

		T::unregisterService();

		_activeClusterPollingServices.fetch_sub(1);
	}

public:

	//! \brief Initialize the Cluster polling services
	//!
	//! This method will be called during ClusterManager
	//! initialization.
	//! New type of polling services need to expose an
	//! initialization interface that will be called from here
	inline static void initialize(bool hybridOnly = false)
	{
		assert(_activeClusterPollingServices.load() == 0);

		assert(MemoryAllocator::isInitialized());

		if (!hybridOnly) {
			assert(ClusterManager::inClusterMode());
			registerService<ClusterPollingServices::MessageHandler<Message>>("MessageHandler");
			registerService<ClusterPollingServices::PendingQueue<Message>>("PendingQueueMessage");
			registerService<ClusterPollingServices::PendingQueue<DataTransfer>>("PendingQueueDataTransfer");
		}
#if HAVE_DLB
		registerService<ClusterPollingServices::HybridPolling>("HybridPolling");
#endif
	}

	inline static void waitUntilFinished()
	{
		ClusterPollingServices::PendingQueue<Message>::waitUntilFinished();
		ClusterPollingServices::PendingQueue<DataTransfer>::waitUntilFinished();
	}

	inline static void setPauseStatus(bool pause)
	{
		_pausedServices.store(pause);

		if (pause == true) {
			waitUntilFinished();
		}
	}

	//! \brief Shutdown the Cluster polling services-
	//!
	//! This method will be called during ClusterManager
	//! shutdown.
	//! New type of polling services need to expose a
	//! shutdown interface that will be called from here.
	inline static void shutdown(bool hybridOnly = false)
	{
		assert(MemoryAllocator::isInitialized());
		assert(_activeClusterPollingServices.load() > 0);

		if (!hybridOnly) {
			// Occasionally a slave node receives the MessageSysFinish and starts
			// the shutdown procedure before the PendingQueue<Message> has checked
			// completion of all the messages it has sent. So just wait for
			// completion before shutting down the polling services.
			waitUntilFinished();
			assert(ClusterManager::inClusterMode());

			unregisterService<ClusterPollingServices::PendingQueue<DataTransfer>>("PendingQueueDataTransfer");
			unregisterService<ClusterPollingServices::PendingQueue<Message>>("PendingQueueMessage");
			unregisterService<ClusterPollingServices::MessageHandler<Message>>("MessageHandler");
		}
#if HAVE_DLB
		unregisterService<ClusterPollingServices::HybridPolling>("HybridPolling");
#endif

		assert(_activeClusterPollingServices.load() == 0);
	}
};


#endif /* CLUSTER_SERVICES_POLLING_HPP */
