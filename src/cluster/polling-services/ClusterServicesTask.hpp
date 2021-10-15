/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_SERVICES_TASK_HPP
#define CLUSTER_SERVICES_TASK_HPP

#include "MessageHandler.hpp"
#include "MessageDelivery.hpp"
#include "ClusterWorker.hpp"

namespace ClusterServicesTask {

	// A polling struct/object must provide 3 static functions:
	// registerService : To start it
	// unregisterService() : To stop it
	// executeService() : The action to execute; it should return true when the service
	// must continue. And false to stop the loop. This function must return false when the service
	// is unregistered and do the needed checks.

	// Defined in ClusterManager.cpp
	extern std::atomic<size_t> _activeClusterTaskServices;

	//! Functions for tasks
	template <typename T>
	static void bodyClusterService(__attribute__((unused)) void *args)
	{
		_activeClusterTaskServices.fetch_add(1);

		while (T::executeService()) {
			nanos6_wait_for(TIMEOUT);
		}

		assert(_activeClusterTaskServices.load() > 0);
		_activeClusterTaskServices.fetch_sub(1);
	}


	template<typename T>
	void registerService(const std::string &name)
	{
		T::registerService();

		const std::string label = "ClusterPolling_" + name;

		SpawnFunction::spawnFunction(
			bodyClusterService<T>,
			nullptr,
			nullptr,
			nullptr,
			label.c_str(),
			false,
			(size_t) Task::nanos6_task_runtime_flag_t::nanos6_polling_flag
		);
	}

	template<typename T>
	void unregisterService()
	{
		T::unregisterService();
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
		assert(_activeClusterTaskServices.load() == 0);

		registerService<ClusterPollingServices::MessageHandler<Message>>("MessageHandler");
		registerService<ClusterPollingServices::PendingQueue<Message>>("MessageDelivery");
		registerService<ClusterPollingServices::PendingQueue<DataTransfer>>("DataTransfer");
		ClusterPollingServices::PendingQueue<DataTransfer>::setEventTypes(Instrument::ClusterEventType::PendingDataTransfersIncoming,
																		  Instrument::ClusterEventType::PendingDataTransfers,
																		  Instrument::ClusterEventType::PendingDataTransferBytes);
	}

	inline void initializeWorkers(int numWorkers)
	{
		for(int i=0; i< numWorkers; i++) {
			registerService<ClusterPollingServices::ClusterWorker>("ClusterWorker");
		}
	}

	inline void waitUntilFinished()
	{
		ClusterPollingServices::PendingQueue<Message>::waitUntilFinished();
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
		assert(_activeClusterTaskServices.load() > 0);

		// Occasionally a slave node receives the MessageSysFinish and starts
		// the shutdown procedure before the PendingQueue<Message> has checked
		// completion of all the messages it has sent. So just wait for
		// completion before shutting down the polling services.
		ClusterPollingServices::PendingQueue<Message>::waitUntilFinished();

		unregisterService<ClusterPollingServices::PendingQueue<DataTransfer>>();
		unregisterService<ClusterPollingServices::PendingQueue<Message>>();
		unregisterService<ClusterPollingServices::MessageHandler<Message>>();

		// Note: shutdownWorkers is always called afterwards
	}

	inline void shutdownWorkers(__attribute__((unused)) int numWorkers)
	{
		for(int i=0; i< numWorkers; i++) {
			unregisterService<ClusterPollingServices::ClusterWorker>();
		}
		// // To assert shutdown the services before the CPUManager
		while (_activeClusterTaskServices.load() > 0) {
			// Wait for cluster polling services before returning
		}
	}
}


#endif /* CLUSTER_SERVICES_TASK_HPP */
