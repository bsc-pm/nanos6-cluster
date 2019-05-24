/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#include <nanos6/polling.h>

#include "MessageHandler.hpp"

#include <ClusterManager.hpp>
#include <InstrumentCluster.hpp>
#include <InstrumentThreadInstrumentationContext.hpp>
#include <Message.hpp>


namespace ClusterPollingServices {
	//! Polling service that checks for incoming messages
	static int messageHandler(void *)
	{
		Message *msg = ClusterManager::checkMail();
		if (msg != nullptr) {

			Instrument::enterHandleReceivedMessage(msg, msg->getSenderId());
			const bool shouldDelete = msg->handleMessage();
			Instrument::exitHandleReceivedMessage(msg);

			if (shouldDelete) {
				delete msg;
			}
		}

		//! Always return false. This will be
		//! unregistered by the ClusterManager
		return 0;
	}

	//! Register service
	void registerMessageHandler()
	{
		//! register message handler
		nanos6_register_polling_service("Cluster message handler", messageHandler, nullptr);
	}

	//! Unregister service
	void unregisterMessageHandler()
	{
		//! unregister message handler
		nanos6_unregister_polling_service("Cluster message handler", messageHandler, nullptr);
	}
}
