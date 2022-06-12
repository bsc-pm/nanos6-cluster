/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_WORKER_HPP
#define CLUSTER_WORKER_HPP

#include <atomic>

#include "lowlevel/PaddedSpinLock.hpp"
#include "system/ompss/SpawnFunction.hpp"

#include "MessageHandler.hpp"

namespace ClusterPollingServices {

	template<typename T>
	class ClusterWorker {

	private:
		static ClusterWorker _singleton;
		std::atomic<int> _live;

	public:
		// When the function returns false the service stops.
		static bool executeService()
		{
			// The service is already unregistered, so finish it.
			if (!_singleton._live.load()) {
				return false;
			}

			T *msg = nullptr;

			// Try to steal a message, and, if successful, handle it
			while ((msg = MessageHandler<T>::stealMessage()) != nullptr) {
				const bool shouldDelete = msg->handleMessage();
				MessageHandler<T>::notifyDone(msg);

				if (shouldDelete) {
					delete msg;
				}
			}

			return true;
		}

		static void registerService()
		{
			_singleton._live.fetch_add(1);
		}

		static void unregisterService()
		{
			_singleton._live.fetch_sub(1);
		}
	};

	template <typename T> ClusterWorker<T> ClusterWorker<T>::_singleton;
}

#endif /* CLUSTER_WORKER_HPP */
