/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef MPI_MESSENGER_HPP
#define MPI_MESSENGER_HPP

#include <sstream>
#include <vector>

#pragma GCC visibility push(default)
#include <mpi.h>
#pragma GCC visibility pop

#include "../Messenger.hpp"

class ClusterPlace;
class DataTransfer;
class Message;

class MPIMessenger : public Messenger {
private:
	// Default value useful for asserts
	int _wrank = -1, _wsize = -1;
	MPI_Comm INTRA_COMM, PARENT_COMM;

	template<typename T>
	void testCompletionInternal(std::vector<T *> &pending);
public:
	MPIMessenger();
	~MPIMessenger();

	void sendMessage(Message *msg, ClusterNode const *toNode, bool block = false);
	void synchronizeAll(void);
	DataTransfer *sendData(const DataAccessRegion &region, const ClusterNode *toNode, int messageId, bool block, bool instrument);
	DataTransfer *fetchData(const DataAccessRegion &region, const ClusterNode *fromNode, int messageId, bool block, bool instrument);
	Message *checkMail();


	inline void testCompletion(std::vector<Message *> &pending)
	{
		testCompletionInternal<Message>(pending);
	}

	inline void testCompletion(std::vector<DataTransfer *> &pending)
	{
		testCompletionInternal<DataTransfer>(pending);
	}

	inline int getNodeIndex() const
	{
		assert(_wrank >= 0);
		return _wrank;
	}

	inline int getMasterIndex() const
	{
		return 0;
	}

	inline int getClusterSize() const
	{
		assert(_wsize > 0);
		return _wsize;
	}

	inline bool isMasterNode() const
	{
		assert(_wrank >= 0);
		return _wrank == 0;
	}
};

//! Register MPIMessenger with the object factory
namespace
{
	Messenger *createMPImsn() { return new MPIMessenger; }

	const bool __attribute__((unused))_registered_MPI_msn =
		REGISTER_MSN_CLASS("mpi-2sided", createMPImsn);
}

#endif /* MPI_MESSENGER_HPP */
