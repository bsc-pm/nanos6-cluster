/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_ACCESS_INFO_HPP
#define MESSAGE_ACCESS_INFO_HPP

#include <sstream>
#include <vector>

#include "Message.hpp"

#include <OffloadedTaskId.hpp>
#include <DataAccessRegion.hpp>
#include "AccessInfo.hpp"

class MessageAccessInfo : public Message {

private:

	struct AccessInfoMessageContent {
		//! The opaque id identifying the offloaded task
		OffloadedTaskIdManager::OffloadedTaskId _offloadedTaskId;
		size_t _numRegions;
		TaskOffloading::AccessInfo _accessInfo[];
	};

	//! \brief pointer to the message payload
	AccessInfoMessageContent *_content;

public:
	MessageAccessInfo(
		size_t numRegions,
		const std::vector<TaskOffloading::AccessInfo> &regions);

	MessageAccessInfo(Deliverable *dlv) : Message(dlv)
	{
		_content = reinterpret_cast<AccessInfoMessageContent *>(_deliverable->payload);
	}

	bool handleMessage() override;

	OffloadedTaskIdManager::OffloadedTaskId getTaskId() const
	{
		return _content->_offloadedTaskId;
	}

	inline std::string toString() const override
	{
		std::stringstream ss;

		const size_t numRegions = _content->_numRegions;
		ss << "[AccessInfo(" << numRegions << "): ";

		for (size_t i = 0; i < numRegions; ++i) {
			ss << "[" << _content->_accessInfo[i]._region << "]";
		}
		ss << "]";

		return ss.str();
	}
};


#endif /* MESSAGE_ACCESS_INFO_HPP */
