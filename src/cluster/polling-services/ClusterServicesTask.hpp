/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_SERVICES_TASK_HPP
#define CLUSTER_SERVICES_TASK_HPP

#include "ClusterWorker.hpp"

class ClusterServicesTask {

	// A polling struct/object must provide 3 static functions:
	// registerService : To start it
	// unregisterService() : To stop it
	// executeService() : The action to execute; it should return true when the service
	// must continue. And false to stop the loop. This function must return false when the service
	// is unregistered and do the needed checks.

	// Defined in ClusterManager.cpp
	static std::atomic<size_t> _activeClusterTaskServices;
	static std::atomic<bool> _pausedServices;

	//! Functions for tasks
	template <typename T>
	static void bodyClusterService(__attribute__((unused)) void *args)
	{
		_activeClusterTaskServices.fetch_add(1);

		while (_pausedServices.load() || T::executeService()) {
			nanos6_wait_for(TIMEOUT);
		}

		assert(_activeClusterTaskServices.load() > 0);
		_activeClusterTaskServices.fetch_sub(1);
	}

	template<typename T>
	static void registerService(const std::string &name)
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
	static void unregisterService()
	{
		T::unregisterService();
	}

public:

	inline static void initializeWorkers(int numWorkers)
	{
		for(int i = 0; i < numWorkers; ++i) {
			registerService<ClusterPollingServices::ClusterWorker<Message>>("ClusterWorker");
		}
	}

	inline static void setPauseStatus(bool pause)
	{
		_pausedServices.store(pause);
	}

	inline static void shutdownWorkers(int numWorkers)
	{
		for(int i = 0; i < numWorkers; ++i) {
			unregisterService<ClusterPollingServices::ClusterWorker<Message>>();
		}
		// To assert shutdown the services before the CPUManager
		// Wait for cluster polling services before returning
		while (_activeClusterTaskServices.load() > 0) {}

		assert(_activeClusterTaskServices == 0);
	}
};


#endif /* CLUSTER_SERVICES_TASK_HPP */
