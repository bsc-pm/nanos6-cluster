/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_SYS_FINISH_HPP
#define MESSAGE_SYS_FINISH_HPP

#include <sstream>

#include "Message.hpp"

class MessageSysFinish : public Message {
public:
	MessageSysFinish(const ClusterNode *from);

	MessageSysFinish(Deliverable *dlv) : Message(dlv)
	{
	}

	bool handleMessage();

	inline std::string toString() const
	{
		return "Node Shutdown";
	}
};

//! Register the Message type to the Object factory
const bool __attribute__((unused))_registered_sys_finish =
	Message::RegisterMSGClass<MessageSysFinish>(SYS_FINISH);

#endif /* MESSAGE_SYS_FINISH_HPP */
