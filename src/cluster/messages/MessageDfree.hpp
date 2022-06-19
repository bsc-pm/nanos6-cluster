/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_DFREE_HPP
#define MESSAGE_DFREE_HPP

#include <sstream>

#include "Message.hpp"
#include <DataAccessRegion.hpp>

class MessageDfree : public Message {
	struct DfreeMessageContent {
		//! address of the distributed allocation
		DataAccessRegion _region;
	};

	//! \brief pointer to the message payload
	DfreeMessageContent *_content;

public:
	MessageDfree(DataAccessRegion &region);

	MessageDfree(Deliverable *dlv) : Message(dlv)
	{
		_content = reinterpret_cast<DfreeMessageContent *>(_deliverable->payload);
	}

	bool handleMessage();

	const DataAccessRegion &getRegion() const
	{
		return _content->_region;
	}

	//! \brief Return a string with a description of the Message
	inline std::string toString() const override
	{
		std::stringstream ss;
		ss << "[dfree:" << _content->_region << "]";

		return ss.str();
	}
};

#endif /* MESSAGE_DFREE_HPP */
