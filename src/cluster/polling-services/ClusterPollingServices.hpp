/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_POLLING_SERVICES_HPP
#define CLUSTER_POLLING_SERVICES_HPP

#include "MessageDelivery.hpp"
#include "MessageHandler.hpp"

namespace ClusterPollingServices {

	// A pooling struct/object must provide 3 static functions:
	// registerService : To start it
	// unregisterService() : To stop it
	// executeService() : The action to execute; it should return true when the service
	// must continue. And false to stop the loop. This function must return false when the service
	// is unregistered and do the needed checks.

	// Declared in ClusterManager.hpp
	extern std::atomic<size_t> _activeClusterPollingServices;

	template <typename T>
	static void bodyCheckDelivery(__attribute__((unused)) void *args)
	{
		_activeClusterPollingServices.fetch_add(1);

		while (PendingQueue<T>::executeService()) {
			nanos6_wait_for(TIMEOUT);
		}

		assert(_activeClusterPollingServices.load() > 0);
		_activeClusterPollingServices.fetch_sub(1);
	}


	template<typename T>
	void registerClusterPolling(const std::string &name)
	{
		PendingQueue<T>::registerService();

		const std::string label = "ClusterPolling_" + name;

		SpawnFunction::spawnFunction(
			bodyCheckDelivery<T>,
			nullptr,
			nullptr,
			nullptr,
			label.c_str(),
			false,
			(size_t) Task::nanos6_task_runtime_flag_t::nanos6_polling_flag
		);
	}

	template<typename T>
	void unregisterClusterPolling()
	{
		PendingQueue<T>::unregisterService();
	}


	//! \brief Initialize the Cluster polling services
	//!
	//! This method will be called during ClusterManager
	//! initialization.
	//! New type of polling services need to expose an
	//! initialization interface that will be called from here
	inline void initialize()
	{
		assert(ClusterManager::inClusterMode());
		assert(MemoryAllocator::isInitialized());

		_activeClusterPollingServices = 0;

		registerMessageHandler();
		registerClusterPolling<Message>("MessageDelivery");
		registerClusterPolling<DataTransfer>("DataTransferDelivery");
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

		unregisterClusterPolling<DataTransfer>();
		unregisterClusterPolling<Message>();
		unregisterMessageHandler();

		// To assert shitdown the services before the CPUManager
		while (_activeClusterPollingServices.load() > 0) {
			// Wait for cluster pooling services before returning
		}
	}
}


#endif /* CLUSTER_POLLING_SERVICES_HPP */
