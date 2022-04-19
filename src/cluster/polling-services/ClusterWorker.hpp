/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_WORKER_HPP
#define CLUSTER_WORKER_HPP

#include <atomic>

#include "lowlevel/PaddedSpinLock.hpp"
#include "system/ompss/SpawnFunction.hpp"

#include <Message.hpp>
#include <ClusterManager.hpp>
#include "MessageHandler.hpp"

class Message;

namespace ClusterPollingServices {

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

			Message *msg = nullptr;

			do {
				// Try to steal a message, and, if successful, handle it
				msg = MessageHandler<Message>::stealMessage();
				if (msg) {
					const bool shouldDelete = msg->handleMessage();
					MessageHandler<Message>::notifyDone(msg);

					if (shouldDelete) {
						delete msg;
					}
				}
			} while (msg);

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
}

#endif /* CLUSTER_WORKER_HPP */
