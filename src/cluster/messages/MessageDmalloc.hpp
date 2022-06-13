/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_DMALLOC_HPP
#define MESSAGE_DMALLOC_HPP

#include <sstream>

#include "Message.hpp"

#include <nanos6/cluster.h>

class MessageDmalloc : public Message {
public:
	struct MessageDmallocDataInfo {
		//! Address pointer.
		void *_dptr;

		//! size in bytes of the requested allocation
		size_t _allocationSize;

		//! Cluster size in allocation moment.
		size_t _clusterSize;

		//! distribution policy for the region
		nanos6_data_distribution_t _policy;

		//! number of dimensions for distribution
		size_t _nrDim;

		//! dimensions of the distribution
		size_t _dimensions[];

		size_t getSize() const
		{
			return sizeof(void*)
				+ 3 * sizeof(size_t)
				+ sizeof(nanos6_data_distribution_t)
				+ _nrDim * sizeof(size_t);
		}

		MessageDmallocDataInfo(
			void *dptr, size_t allocationSize, size_t clusterSize,
			nanos6_data_distribution_t policy, size_t nrDim, size_t *dimensions
		) : _dptr(dptr), _allocationSize(allocationSize), _clusterSize(clusterSize),
			_policy(policy), _nrDim(nrDim)
		{
			memcpy(_dimensions, dimensions, sizeof(size_t) * nrDim);
		}
	};


	// The content will have {nallocs, offsets[nallocs], data[nallocs]} but data is actually an
	// array of variable size elements, so each element starts in the offset[i] position.
	struct DmallocMessageContent {
		size_t _ndmallocs;
		char _msgData[];

		size_t *getOffsetPtr() const
		{
			return (size_t*)_msgData;
		}

		MessageDmallocDataInfo *getData(size_t idx)
		{
			assert(idx < _ndmallocs);

			const size_t offset = _ndmallocs * sizeof(size_t) + this->getOffsetPtr()[idx];

			return reinterpret_cast<MessageDmallocDataInfo *>(_msgData + offset);
		}
	};


private:
	//! \brief pointer to the message payload
	DmallocMessageContent *_content;

public:
	MessageDmalloc(const ClusterNode *from,
		void *dptr, size_t size, size_t clusterSize, nanos6_data_distribution_t policy,
		size_t numDimensions, size_t *dimensions
	);

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

		for (size_t i = 0; i < _content->_ndmallocs; ++i) {
			MessageDmallocDataInfo *info = _content->getData(i);
			ss << "[dmalloc:"<< info->_dptr << ":" << info->_allocationSize << "]";
		}

		return ss.str();
	}
};

#endif /* MESSAGE_DMALLOC_HPP */
