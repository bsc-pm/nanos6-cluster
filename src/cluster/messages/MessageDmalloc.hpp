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
	struct DmallocMessageContent {
		//! Address pointer.
		void *_dptr;

		//! size in bytes of the requested allocation
		size_t _allocationSize;

		//! distribution policy for the region
		nanos6_data_distribution_t _policy;

		//! number of dimensions for distribution
		size_t _nrDim;

		//! dimensions of the distribution
		size_t _dimensions[];
	};

	//! \brief pointer to the message payload
	DmallocMessageContent *_content;

public:
	MessageDmalloc(const ClusterNode *from,
		void *dptr, size_t size, nanos6_data_distribution_t policy,
		size_t numDimensions, size_t *dimensions
	);

	MessageDmalloc(Deliverable *dlv) : Message(dlv)
	{
		_content = reinterpret_cast<DmallocMessageContent *>(_deliverable->payload);
	}

	bool handleMessage();

	inline void *getPointer() const
	{
		return _content->_dptr;
	}

	inline void setPointer(void *dptr)
	{
		_content->_dptr = dptr;
	}

	//! \brief Get the allocation size
	inline size_t getAllocationSize() const
	{
		return _content->_allocationSize;
	}

	//! \brief Get distribution policy
	inline nanos6_data_distribution_t getDistributionPolicy() const
	{
		return _content->_policy;
	}

	//! \brief Get policy dimensions size
	inline size_t getDimensionsSize() const
	{
		return _content->_nrDim;
	}

	//! \brief Get policy dimensions
	inline size_t *getDimensions() const
	{
		if (_content->_nrDim == 0) {
			return nullptr;
		}

		return _content->_dimensions;
	}

	//! \brief Return a string with a description of the Message
	inline std::string toString() const
	{
		std::stringstream ss;

		ss << "[size:" << _content->_allocationSize << "]";

		return ss.str();
	}
};

#endif /* MESSAGE_DMALLOC_HPP */
