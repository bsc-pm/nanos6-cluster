/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_HPP
#define MESSAGE_HPP

#include <string>

#include "MessageType.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "lowlevel/threads/KernelLevelThread.hpp"
#include "support/GenericFactory.hpp"

class ClusterNode;

class Message {
public:
	struct msg_header {
		//! the type of the message
		MessageType type;

		//! size of the payload in bytes
		size_t size;

		//! Id of the message
		int id;

		//! Cluster index of the sender node
		int senderId;
	};

	//! Deliverable is the structure that is actually sent over the network.
	//!
	//! It contains a message header and a payload that is MessageType specific.
	//! This struct is sent as is over the network without any serialisation.
	typedef struct {
		struct msg_header header;
		char payload[];
	} Deliverable;

	typedef std::function<void ()> message_callback_t;

private:
	//! The callback that we will invoke when the DataTransfer completes
	std::vector<message_callback_t> _callbacks;

	//! An opaque pointer to Messenger-specific data
	void * _messengerData;

	//! Flag indicating whether the Message has been delivered
	bool _completed;

protected:
	//! The part of the message we actually send over the network
	Deliverable *_deliverable;

public:
	Message() = delete;
	Message(MessageType type, size_t size, const ClusterNode *from);

	template<typename T>
	static bool RegisterMSGClass(int id)
	{
		static_assert(std::is_base_of<Message, T>::value, "Base class is wrong.");

		return GenericFactory<int, Message*, Message::Deliverable*>::getInstance().emplace(
			id,
			[](Message::Deliverable *dlv) -> Message* {
				return new T(dlv);
			}
		);
	}



	//! Construct a message from a received(?) Deliverable structure
	Message(Deliverable *dlv)
		: _callbacks(), _messengerData(nullptr), _completed(false),
		_deliverable(dlv)
	{
		assert(dlv != nullptr);
	}

	virtual ~Message()
	{
		assert(_deliverable != nullptr);
		free(_deliverable);
	}

	inline const std::string getName() const
	{
		return std::string(MessageTypeStr[_deliverable->header.type]);
	}

	//! \brief Returns the type of the Message
	inline MessageType getType() const
	{
		return _deliverable->header.type;
	}

	//! \brief Returns the size of the Message
	inline size_t getSize() const
	{
		return _deliverable->header.size;
	}

	//! \brief Returns the id of the Message
	inline int getId() const
	{
		return _deliverable->header.id;
	}

	//! \brief Returns the cluster id of the node that sent the  Message
	inline int getSenderId() const
	{
		return _deliverable->header.senderId;
	}

	//! \brief Return the Deliverable data of the Message
	inline Deliverable *getDeliverable() const
	{
		return _deliverable;
	}

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
		for (message_callback_t callback : _callbacks) {
			callback();
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
	inline void addCompletionCallback(message_callback_t callback)
	{
		_callbacks.push_back(callback);
	}

	//! \brief Handles the received message
	//!
	//! Specific to each type of message. This method handles the
	//! MessageType specific operation. It returns true if the Message
	//! can be delete or false otherwise.
	virtual bool handleMessage() = 0;

	//! \brief prints info about the message
	virtual std::string toString() const = 0;

	friend std::ostream& operator<<(std::ostream& out, const Message& msg)
	{
		out << msg.toString();
		return out;
	}
};


#endif /* MESSAGE_HPP */
