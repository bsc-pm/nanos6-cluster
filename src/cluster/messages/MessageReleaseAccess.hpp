/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_RELEASE_ACCESS_HPP
#define MESSAGE_RELEASE_ACCESS_HPP

#include <sstream>

#include "Message.hpp"
#include "dependencies/DataAccessType.hpp"

#include <DataAccessRegion.hpp>
#include "WriteID.hpp"

class MessageReleaseAccess : public Message {
public:

	struct ReleaseAccessInfo {
		//! The region we are releasing
		DataAccessRegion _region;

		WriteID _writeID;

		//! The location on which the access is being released
		int _location;

		ReleaseAccessInfo(
			const DataAccessRegion &region,
			WriteID writeID,
			const MemoryPlace *location
		) : _region(region.getStartAddress(), region.getSize()),
			_writeID(writeID),
			_location(location->getIndex())
		{
			// The location should be either a cluster node or the
			// directory (which would mean uninitialized data, maybe "all memory")
			assert(location->getType() == nanos6_cluster_device
					|| location->isDirectoryMemoryPlace());
		}

		friend std::ostream& operator<<(std::ostream &o, const ReleaseAccessInfo &info)
		{
			o << "[RelReg:" << info._region << " loc:" << info._location << "]";
			return o;
		}
	};

	typedef std::vector<MessageReleaseAccess::ReleaseAccessInfo> ReleaseAccessInfoVector;

private:
	struct ReleaseAccessMessageContent {
		//! The opaque id identifying the offloaded task
		OffloadedTaskIdManager::OffloadedTaskId _offloadedTaskId;
		size_t _release;
		size_t _ninfos;
		ReleaseAccessInfo _regionInfoList[];
	};

	//! pointer to message payload
	ReleaseAccessMessageContent *_content;

public:

	MessageReleaseAccess(
		OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId,
		bool release,
		ReleaseAccessInfoVector &vector
	);

	MessageReleaseAccess(Deliverable *dlv) : Message(dlv)
	{
		_content = reinterpret_cast<ReleaseAccessMessageContent *>(_deliverable->payload);
	}

	bool handleMessage();

	OffloadedTaskIdManager::OffloadedTaskId getTaskId() const
	{
		return _content->_offloadedTaskId;
	}

	inline std::string toString() const override
	{
		std::stringstream ss;

		const size_t nRegions = _content->_ninfos;
		ss << "[ReleaseAccess(" << nRegions << "):";

		for (size_t i = 0; i < nRegions; ++i) {
			ss << "[" << _content->_regionInfoList[i] << "]";
		}
		ss << "]";

		return ss.str();
	}
};

#endif /* MESSAGE_RELEASE_ACCESS_HPP */
