/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_DATA_SEND_HPP
#define MESSAGE_DATA_SEND_HPP

#include <sstream>

#include "Message.hpp"

#include <DataAccessRegion.hpp>
#include <DataSendInfo.hpp>

class MessageDataSend : public Message {

	// Similar to TaskOffloading::DataSendRegionInfo, but using the integer recipient node
	struct DataSendRegionInfoElem {

		//! The remote region we update
		DataAccessRegion _remoteRegion;

		//! Node that the data should be sent to
		short _recipientNodeIndex;

		//! Id of the resulting data transfer message
		int _id;
	};

	struct DataSendMessageContent {
		size_t _numSends;
		DataSendRegionInfoElem _dataSendRegionInfo[];
	};

	//! \brief pointer to the message payload
	DataSendMessageContent *_content;

public:
	MessageDataSend(
		size_t numSends,
		std::vector<TaskOffloading::DataSendRegionInfo> const &dataSends);

	MessageDataSend(Deliverable *dlv) : Message(dlv)
	{
		_content = reinterpret_cast<DataSendMessageContent *>(_deliverable->payload);
	}

	bool handleMessage() override;

	inline std::string toString() const override
	{
		std::stringstream ss;

		const size_t numSends = _content->_numSends;
		ss << "[DataSend(" << numSends << "):";

		for (size_t i = 0; i < numSends; ++i) {
			ss << "[" << _content->_dataSendRegionInfo[i]._remoteRegion << "]";
		}
		ss << "]";

		return ss.str();
	}
};


#endif /* MESSAGE_DATA_SEND_HPP */
