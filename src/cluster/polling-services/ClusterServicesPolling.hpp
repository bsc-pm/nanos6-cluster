/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_SERVICES_POLLING_HPP
#define CLUSTER_SERVICES_POLLING_HPP

#include "MessageHandler.hpp"
#include "MessageDelivery.hpp"

namespace ClusterServicesPolling {

	// Defined in ClusterManager.cpp
	extern std::atomic<size_t> _activeClusterPollingServices;

	template <typename T>
	static int bodyClusterService(__attribute__((unused)) void *args)
	{
		// This returns true unless the service is unregistered
		T::executeService();
		return 0;
	}

	template<typename T>
	void registerService(const std::string &name)
	{
		const std::string label = "ClusterPolling_" + name;
		_activeClusterPollingServices.fetch_add(1);

		T::registerService();

		nanos6_register_polling_service(label.c_str(), bodyClusterService<T>, nullptr);
	}

	template<typename T>
	void unregisterService(const std::string &name)
	{
		const std::string label = "ClusterPolling_" + name;
		nanos6_unregister_polling_service(label.c_str(), bodyClusterService<T>, nullptr);

		T::unregisterService();

		_activeClusterPollingServices.fetch_sub(1);
	}

	//! \brief Initialize the Cluster polling services
	//!
	//! This method will be called during ClusterManager
	//! initialization.
	//! New type of polling services need to expose an
	//! initialization interface that will be called from here
	inline void initialize()
	{
		assert(_activeClusterPollingServices.load() == 0);

		assert(ClusterManager::inClusterMode());
		assert(MemoryAllocator::isInitialized());

		registerService<ClusterPollingServices::MessageHandler<Message>>("MessageHandler");
		registerService<ClusterPollingServices::PendingQueue<Message>>("MessageDelivery");
		registerService<ClusterPollingServices::PendingQueue<DataTransfer>>("DataTransfer");
		ClusterPollingServices::PendingQueue<DataTransfer>::setEventTypes(Instrument::ClusterEventType::PendingDataTransfersIncoming,
																		  Instrument::ClusterEventType::PendingDataTransfers,
																		  Instrument::ClusterEventType::PendingDataTransferBytes);
	}

	//! \brief Shutdown the Cluster polling services-
	//!
	//! This method will be called during ClusterManager
	//! shutdown.
	//! New type of polling services need to expose a
	//! shutdown interface that will be called from here.
	inline void shutdown()
	{
		assert(ClusterManager::inClusterMode());
		assert(MemoryAllocator::isInitialized());
		assert(_activeClusterPollingServices.load() > 0);

		// Occasionally a slave node receives the MessageSysFinish and starts
		// the shutdown procedure before the PendingQueue<Message> has checked
		// completion of all the messages it has sent. So just wait for
		// completion before shutting down the polling services.
		ClusterPollingServices::PendingQueue<Message>::waitUntilFinished();

		unregisterService<ClusterPollingServices::PendingQueue<DataTransfer>>("DataTransfer");
		unregisterService<ClusterPollingServices::PendingQueue<Message>>("MessageDelivery");
		unregisterService<ClusterPollingServices::MessageHandler<Message>>("MessageHandler");

		assert(_activeClusterPollingServices.load() == 0);
	}
}


#endif /* CLUSTER_SERVICES_POLLING_HPP */
