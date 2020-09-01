/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef HYBRID_POLLING_HPP
#define HYBRID_POLLING_HPP

#include <ClusterManager.hpp>

namespace ClusterPollingServices {

	class HybridPolling {
		std::atomic<bool> _live;
		static HybridPolling _singleton;

	public:

		// When the function returns false the service stops.
		static bool executeService()
		{
			// The service is already unregistered, so finish it.
			if (!_singleton._live.load()) {
				return false;
			}
			ClusterManager::hybridInterfacePoll();
			return true;
		}

		static void registerService()
		{
			assert(_singleton._live.load() == false);
			_singleton._live = true;
		}

		static void unregisterService()
		{
			assert(_singleton._live.load() == true);
			_singleton._live = false;
		}
	};
}

#endif /* HYBRID_POLLING_HPP */
