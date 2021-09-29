/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_DATA_SEND_HPP
#define MESSAGE_DATA_SEND_HPP

#include <sstream>

#include "Message.hpp"

#include <DataAccessRegion.hpp>

class MessageDataSend : public Message {
	struct DataSendMessageContent {
		//! The remote region we update
		DataAccessRegion _remoteRegion;

		//! Node that the data should be sent to
		short _recipientNodeIndex;

		//! Id of the resulting data transfer message
		int _id;
	};

	//! \brief pointer to the message payload
	DataSendMessageContent *_content;

public:
	MessageDataSend(const ClusterNode *from,
		DataAccessRegion const &remoteRegion,
		ClusterNode *recipient,
		int id);

	MessageDataSend(Deliverable *dlv)
		: Message(dlv)
	{
		_content = reinterpret_cast<DataSendMessageContent *>(_deliverable->payload);
	}

	bool handleMessage();

	inline std::string toString() const
	{
		std::stringstream ss;

		ss << "[region:" << _content->_remoteRegion << "]";

		return ss.str();
	}
};


#endif /* MESSAGE_DATA_SEND_HPP */
