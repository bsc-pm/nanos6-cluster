/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef DATA_TRANSFER_HPP
#define DATA_TRANSFER_HPP

#include <functional>

#include "hardware/places/MemoryPlace.hpp"

#include <DataAccessRegion.hpp>

class MemoryPlace;

class DataTransfer {
public:
	//! The region that is being transfered
	DataAccessRegion _region;

	//! Source memory place
	MemoryPlace const *_source;

	//! Target memory place
	MemoryPlace const *_target;

private:
	typedef std::function<void ()> data_transfer_callback_t;

	//! The callback that we will invoke when the DataTransfer completes
	std::vector<data_transfer_callback_t> _callbacks;

	//! Flag indicating DataTransfer completion
	bool _completed;

	//! An opaque pointer to Messenger-specific data
	void * _messengerData;

public:
	DataTransfer(
		DataAccessRegion const &region,
		MemoryPlace const *source,
		MemoryPlace const *target,
		void *messengerData
	) : _region(region), _source(source), _target(target),
		_callbacks(), _completed(false), _messengerData(messengerData)
	{
	}

	virtual ~DataTransfer()
	{
	}

	//! \brief Set the callback for the DataTransfer
	//!
	//! \param[in] callback is the completion callback
	inline void addCompletionCallback(data_transfer_callback_t callback)
	{
		_callbacks.push_back(callback);
	}

	//! \brief Mark the DataTransfer as completed
	//!
	//! If there is a valid callback assigned to the DataTransfer it will
	//! be invoked
	inline void markAsCompleted()
	{
		for(data_transfer_callback_t callback : _callbacks) {
			callback();
		}

		_completed = true;
	}

	//! \brief Check if the DataTransfer is completed
	inline bool isCompleted() const
	{
		return _completed;
	}

	//! \brief Return the Messenger-specific data
	inline void *getMessengerData() const
	{
		return _messengerData;
	}

	friend std::ostream& operator<<(std::ostream &out, const DataTransfer &dt)
	{
		out << "DataTransfer from: " <<
			dt._source->getIndex() << " to: " <<
			dt._target->getIndex() << " region:" <<
			dt._region;
		return out;
	}
};


#endif /* DATA_TRANSFER_HPP */
