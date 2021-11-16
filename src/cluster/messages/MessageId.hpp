/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_ID_HPP
#define MESSAGE_ID_HPP

#include <atomic>

#include "MessageType.hpp"

namespace MessageId {

	extern uint32_t _numRanks;

	//! \brief Initialize globally unique MessageIds
	void initialize(int rank, int numRanks);

	//! \brief Get the next available MessageId
	//!
	//! \param[in] numIds The number of Ids to allocate.
	//!
	//! \returns The first MessageId. Use messageIdAtOffset to find
	//! the other MessageIds, if numIds > 1.
	uint32_t nextMessageId(int numIds = 1);

	//! \brief Extract a MessageId from a group
	//!
	//! \param[in] firstMessageId The first MessageId, returned by
	//!            nextMessageId.
	//! \param[in] ofs The index, counting from zero.
	inline uint32_t messageIdInGroup(int firstMessageId, int ofs)
	{
		return firstMessageId + ofs * _numRanks;
	}
}

#endif /* MESSAGE_ID_HPP */
