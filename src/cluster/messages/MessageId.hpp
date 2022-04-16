/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_ID_HPP
#define MESSAGE_ID_HPP

#include <atomic>

#include "MessageType.hpp"

/* NOTE: The message ID is globally unique across all messages in the whole
 * program (i.e. on all nodes). This seems wasteful but it allows a
 * DATA_RAW message to use the same ID as the associated message, even
 * when:
 *
 *   1) The DATA_RAW messages are responses to different message types sent
 *   by the same node, e.g. the first is in response to a MessageDmalloc
 *   from a slave and the second is in response to a MessageDataFetch. To
 *   ensure that these DATA_RAW messages do not have the same ID and type
 *   (DATA_RAW), it is most convenient to make all messages sent from the
 *   same node have different IDs.
 *
 *   2) The DATA_RAW messages are sent from the same source to the same
 *   destination, but they are associated with messages sent in opposite
 *   directions, e.g. the first accompanies a MessageDmalloc from the
 *   master and the second is in response to a MessageDataFetch. Since the
 *   message IDs are allocated on different nodes they could easily
 *   collide.  It could be solved using different DATA_RAW types (for
 *   DMALLOCs in both directions, data fetches and data sends) or a flag
 *   saying whether the DATA_RAW is in the same or opposite sense as the
 *   original message, but both of these seem unnecessarily complex.
 *
 * Uniqueness of IDs is important for two reasons:
 *
 *   1) MPI message matching. The field for the message ID in the MPI tag
 *   is 24 bits, but it only needs to be unique among messages in flight at
 *   the same time. Hence overflow is not important. This assumes an 8-bit
 *   message type field, which could be cut to e.g. 5 bit if needed.
 *
 *   2) Extrae instrumentation. The Extrae ID needs to be globally unique,
 *   but it is of type long long. The id field in the Message type is 32
 *   bits and could easily be extended to 64 bits if it becomes a problem.
 */


class MessageId {

private:
	uint32_t _numRanks;
	std::atomic<uint32_t> _nextMessageId;

	static MessageId *_singleton;

	MessageId() : _numRanks(0), _nextMessageId(0)
	{}

	void resetInternal(int rank, int numRanks)
	{
		assert(_nextMessageId == 0);
		_nextMessageId = rank + 256;
		_numRanks = numRanks;
	}

	uint32_t nextMessageIdInternal(int numIds = 1)
	{
		assert(_singleton != nullptr);
		assert(numIds >= 1);
		assert(_numRanks != 0);
		const uint32_t ret = _nextMessageId.fetch_add(numIds * _numRanks);

		/* Check for overflow */
		assert(ret != UINT_MAX);

		return ret;
	}

public:

	//! \brief Initialize globally unique MessageIds
	inline static void initialize(int rank, int numRanks)
	{
		assert(_singleton == nullptr);
		_singleton = new MessageId();
		MessageId::reset(rank, numRanks);
	}

	inline static void reset(int rank, int numRanks)
	{
		assert(_singleton != nullptr);
		_singleton->resetInternal(rank, numRanks);
	}

	//! \brief Get the next available MessageId
	//!
	//! \param[in] numIds The number of Ids to allocate.
	//!
	//! \returns The first MessageId. Use messageIdAtOffset to find
	//! the other MessageIds, if numIds > 1.
	inline static uint32_t nextMessageId(int numIds = 1)
	{
		assert(_singleton != nullptr);
		return _singleton->nextMessageIdInternal(numIds);
	}

	//! \brief Extract a MessageId from a group
	//!
	//! \param[in] firstMessageId The first MessageId, returned by
	//!            nextMessageId.
	//! \param[in] ofs The index, counting from zero.
	inline static uint32_t messageIdInGroup(int firstMessageId, int ofs)
	{
		assert(_singleton != nullptr);
		return firstMessageId + ofs * _singleton->_numRanks;
	}
};

#endif /* MESSAGE_ID_HPP */
