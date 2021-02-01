/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_DATA_FETCH_HPP
#define MESSAGE_DATA_FETCH_HPP

#include <sstream>

#include "Message.hpp"
#include "MessageId.hpp"

#include <DataAccessRegion.hpp>

class MessageDataFetch : public Message {
public:
	struct DataAccessRegionInfo {
		int _id;                         // Reply messageID
		DataAccessRegion _remoteRegion;  // Region fragment
	};

	struct DataFetchMessageContent {
		//! The remote region we bring data from
		size_t _nregions;
		DataAccessRegionInfo _remoteRegionInfo[];
	};

private:
	//! \brief pointer to the message payload
	DataFetchMessageContent *_content;

public:
	MessageDataFetch(const ClusterNode *from, DataAccessRegion const &remoteRegion);

	MessageDataFetch(Deliverable *dlv) : Message(dlv)
	{
		_content = reinterpret_cast<DataFetchMessageContent *>(_deliverable->payload);
	}

	bool handleMessage();

	inline std::string toString() const
	{
		std::stringstream ss;
		const size_t nregions = _content->_nregions;
		ss << "[regions(" << nregions << "): ";

		for (size_t i = 0; i < nregions; ++i) {
			ss << _content->_remoteRegionInfo[i]._remoteRegion
				<< (i < nregions - 1 ? "; " : "]");
		}

		return ss.str();
	}

	DataFetchMessageContent *getContent() const
	{
		return _content;
	}

	static size_t getMPIFragments(DataAccessRegion const &remoteRegion);

	static size_t getMessageContentSizeFromRegion(DataAccessRegion const &remoteRegion)
	{
		const size_t nFragments = getMPIFragments(remoteRegion);
		assert(nFragments > 0);

		return sizeof(size_t) + (nFragments * sizeof(DataAccessRegionInfo));
	}
};

#endif /* MESSAGE_DATA_FETCH_HPP */
