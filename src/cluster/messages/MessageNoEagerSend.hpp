/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_NO_EAGER_SEND_SEND_HPP
#define MESSAGE_NO_EAGER_SEND_SEND_HPP

#include <sstream>
#include <vector>

#include "Message.hpp"

#include <DataAccessRegion.hpp>
#include "NoEagerSendInfo.hpp"

class MessageNoEagerSend : public Message {

private:

	struct NoEagerSendMessageContent {
		size_t _numRegions;
		TaskOffloading::NoEagerSendInfo _noEagerSendInfo[];
	};

	//! \brief pointer to the message payload
	NoEagerSendMessageContent *_content;

public:
	MessageNoEagerSend(const ClusterNode *from,
		size_t numRegions,
		const std::vector<TaskOffloading::NoEagerSendInfo> &regions);

	MessageNoEagerSend(Deliverable *dlv)
		: Message(dlv)
	{
		_content = reinterpret_cast<NoEagerSendMessageContent *>(_deliverable->payload);
	}

	bool handleMessage();

	inline std::string toString() const
	{
		std::stringstream ss;

		const size_t numRegions = _content->_numRegions;
		ss << "[regions(" << numRegions << "): ";

		for (size_t i = 0; i < numRegions; ++i) {
			ss << _content->_noEagerSendInfo[i]._region
				<< (i < numRegions - 1 ? "; " : "]");
		}

		return ss.str();
	}
};


#endif /* MESSAGE_NO_EAGER_SEND_SEND_HPP */
