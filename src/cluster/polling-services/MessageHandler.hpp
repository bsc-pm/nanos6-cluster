/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_HANDLER_HPP
#define MESSAGE_HANDLER_HPP

#include "lowlevel/PaddedSpinLock.hpp"
#include "MessageHandler.hpp"

#include <ClusterManager.hpp>
#include <InstrumentCluster.hpp>
#include <InstrumentThreadInstrumentationContext.hpp>
#include <Message.hpp>


namespace ClusterPollingServices {

	template<typename T>
	class MessageHandler {
	private:
		PaddedSpinLock<> _lock;
		std::atomic<bool> _live;

		static MessageHandler<T> _singleton;

	public:

		// When the function returns false the service stops.
		static bool executeService()
		{
			// The service is already unregistered, so finish it.
			if (!_singleton._live.load()) {
				return false;
			}

			T *msg = ClusterManager::checkMail();

			if (msg != nullptr) {
				Instrument::enterHandleReceivedMessage(msg, msg->getSenderId());
				const bool shouldDelete = msg->handleMessage();
				Instrument::exitHandleReceivedMessage(msg);

				if (shouldDelete) {
					delete msg;
				}
			}

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

	template <typename T> MessageHandler<T> MessageHandler<T>::_singleton;
}

#endif /* MESSAGE_HANDLER_HPP */
