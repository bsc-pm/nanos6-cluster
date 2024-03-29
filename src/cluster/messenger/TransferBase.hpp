/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef TRANSFER_BASE_HPP
#define TRANSFER_BASE_HPP

#include <atomic>
#include <functional>
#include <map>
#include <cassert>

class TransferBase {
public:
	typedef std::function<void ()> transfer_callback_t;

private:
	//! The callback that we will invoke when the DataTransfer completes.  The int will be used as a
	// priority and the std::greater function asserts that the entries are iterated in reverse order
	// (so higher values first). This enables to define the order of the callbacks..
	std::multimap<int, transfer_callback_t, std::greater<int>> _callbacks;

	//! An opaque pointer to Messenger-specific data
	void * _messengerData;

	//! Flag indicating whether the Message has been delivered
	bool _completed;

public:

	TransferBase(void *messengerData)
		: _callbacks(), _messengerData(messengerData),_completed(false)
	{}

	virtual ~TransferBase()
	{}

	//! \brief Return the Messenger-specific data
	inline void *getMessengerData() const
	{
		return _messengerData;
	}

	//! \brief Set the Messenger-specific data
	inline void setMessengerData(void *data)
	{
		_messengerData = data;
	}

	//! \brief Mark the Message as delivered
	inline void markAsCompleted()
	{
		// this function executes the callbacks; they are executed in priority order from higher
		// priority to lower. callbacks with same priority are executed in any order between them.
		for (auto callback : _callbacks) {
			callback.second();
		}

		_completed = true;
	}

	//! \brief Check if the Message is delivered
	inline bool isCompleted() const
	{
		return _completed;
	}

	//! \brief Set the callback for the Message
	//!
	//! \param[in] callback is the completion callback
	inline void addCompletionCallback(transfer_callback_t callback, int priority = 0)
	{
		_callbacks.insert({priority, callback});
	}
};

#endif // TRANSFER_BASE_HPP
