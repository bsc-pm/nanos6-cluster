/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_NO_EAGER_SEND_SEND_HPP
#define MESSAGE_NO_EAGER_SEND_SEND_HPP

#include <sstream>
#include <vector>

#include "Message.hpp"

#include <OffloadedTaskId.hpp>
#include <DataAccessRegion.hpp>
#include "NoEagerSendInfo.hpp"

class MessageNoEagerSend : public Message {

private:

	struct NoEagerSendMessageContent {
		//! The opaque id identifying the offloaded task
		OffloadedTaskIdManager::OffloadedTaskId _offloadedTaskId;
		size_t _numRegions;
		TaskOffloading::NoEagerSendInfo _noEagerSendInfo[];
	};

	//! \brief pointer to the message payload
	NoEagerSendMessageContent *_content;

public:
	MessageNoEagerSend(
		size_t numRegions,
		const std::vector<TaskOffloading::NoEagerSendInfo> &regions);

	MessageNoEagerSend(Deliverable *dlv) : Message(dlv)
	{
		_content = reinterpret_cast<NoEagerSendMessageContent *>(_deliverable->payload);
	}

	bool handleMessage();

	OffloadedTaskIdManager::OffloadedTaskId getTaskId() const
	{
		return _content->_offloadedTaskId;
	}

	inline std::string toString() const override
	{
		std::stringstream ss;

		const size_t numRegions = _content->_numRegions;
		ss << "[NoEagerSend(" << numRegions << "): ";

		for (size_t i = 0; i < numRegions; ++i) {
			ss << "[" << _content->_noEagerSendInfo[i]._region << "]";
		}
		ss << "]";

		return ss.str();
	}
};


#endif /* MESSAGE_NO_EAGER_SEND_SEND_HPP */
