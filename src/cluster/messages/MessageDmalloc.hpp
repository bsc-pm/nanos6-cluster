/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_DMALLOC_HPP
#define MESSAGE_DMALLOC_HPP

#include <sstream>

#include "Message.hpp"
#include "ClusterMemoryManagement.hpp"

#include <nanos6/cluster.h>

class MessageDmalloc : public Message {
public:
	// The content will have {nallocs, offsets[nallocs], data[nallocs]} but data is actually an
	// array of variable size elements, so each element starts in the offset[i] position.
	struct DmallocMessageContent {
		size_t _ndmallocs;
		char _msgData[];

		size_t *getOffsetPtr() const
		{
			return (size_t*)_msgData;
		}

		ClusterMemoryManagement::DmallocDataInfo *getData(size_t idx)
		{
			assert(idx < _ndmallocs);

			const size_t offset = _ndmallocs * sizeof(size_t) + this->getOffsetPtr()[idx];

			return reinterpret_cast<ClusterMemoryManagement::DmallocDataInfo *>(_msgData + offset);
		}
	};


private:
	//! \brief pointer to the message payload
	DmallocMessageContent *_content;

public:
	MessageDmalloc(
		const DataAccessRegion &region,
		size_t clusterSize,
		nanos6_data_distribution_t policy,
		size_t numDimensions,
		const size_t *dimensions
	);

	MessageDmalloc(std::list<ClusterMemoryManagement::DmallocDataInfo *> &dmallocs);

	MessageDmalloc(Deliverable *dlv) : Message(dlv)
	{
		_content = reinterpret_cast<DmallocMessageContent *>(_deliverable->payload);
	}

	bool handleMessage();

	DmallocMessageContent *getContent() const
	{
		return _content;
	}

	//! \brief Return a string with a description of the Message
	inline std::string toString() const
	{
		std::stringstream ss;

		ss << "[dmalloc:";
		for (size_t i = 0; i < _content->_ndmallocs; ++i) {
			ss << "(" << _content->getData(i)->_region << ")";
		}
		ss << "]";

		return ss.str();
	}
};

#endif /* MESSAGE_DMALLOC_HPP */
