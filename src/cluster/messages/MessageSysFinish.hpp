/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_SYS_FINISH_HPP
#define MESSAGE_SYS_FINISH_HPP

#include <sstream>

#include "Message.hpp"
#include "NodeNamespace.hpp"
#include "ClusterManager.hpp"

class MessageSysFinish : public Message {
public:

	MessageSysFinish() : Message(SYS_FINISH, 1)
	{}

	MessageSysFinish(Deliverable *dlv) : Message(dlv)
	{
	}

	bool handleMessage()
	{
		do {} while (!NodeNamespace::isEnabled());

		NodeNamespace::notifyShutdown();
		ClusterManager::synchronizeAll();

		return true;
	}

	inline std::string toString() const override
	{
		return "[Shutdown]";
	}
};

//! Register the Message type to the Object factory
const bool __attribute__((unused))_registered_sys_finish =
	Message::RegisterMSGClass<MessageSysFinish>(SYS_FINISH);

#endif /* MESSAGE_SYS_FINISH_HPP */
