/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_NO_EAGER_SEND_SEND_HPP
#define MESSAGE_NO_EAGER_SEND_SEND_HPP

#include <sstream>

#include "Message.hpp"

#include <DataAccessRegion.hpp>

class MessageNoEagerSend : public Message {
	struct NoEagerSendMessageContent {
		//! The region we do not access
		DataAccessRegion _region;

		//! The opaque id identifying the offloaded task
		void *_offloadedTaskId;
	};

	//! \brief pointer to the message payload
	NoEagerSendMessageContent *_content;

public:
	MessageNoEagerSend(const ClusterNode *from,
		DataAccessRegion const &remoteRegion,
		void *offloadedTaskId);

	MessageNoEagerSend(Deliverable *dlv)
		: Message(dlv)
	{
		_content = reinterpret_cast<NoEagerSendMessageContent *>(_deliverable->payload);
	}

	bool handleMessage();

	inline std::string toString() const
	{
		std::stringstream ss;

		ss << "[region:" << _content->_region << "]";

		return ss.str();
	}
};


#endif /* MESSAGE_NO_EAGER_SEND_SEND_HPP */
