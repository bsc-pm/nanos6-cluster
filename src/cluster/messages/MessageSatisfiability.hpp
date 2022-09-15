/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_SATISFIABILITY_HPP
#define MESSAGE_SATISFIABILITY_HPP

#include "Message.hpp"

class MessageSatisfiability : public Message {
private:
	struct SatisfiabilityMessageContent {
		size_t _nSatisfiabilities;
		TaskOffloading::SatisfiabilityInfo _SatisfiabilityInfo[];
	};

	//! pointer to message payload
	SatisfiabilityMessageContent *_content;

public:
	MessageSatisfiability(TaskOffloading::SatisfiabilityInfoVector &satInfo);

	MessageSatisfiability(Deliverable *dlv) : Message(dlv)
	{
		_content = reinterpret_cast<SatisfiabilityMessageContent *>(_deliverable->payload);
	}

	bool handleMessage() override;

	inline std::string toString() const override
	{
		std::stringstream ss;

		const size_t nRegions = _content->_nSatisfiabilities;
		ss << "Satisfiability(" << nRegions << "):";

		for (size_t i = 0; i < nRegions; ++i) {
			ss << _content->_SatisfiabilityInfo[i];
		}

		return ss.str();
	}
};

#endif /* MESSAGE_SATISFIABILITY_HPP */
