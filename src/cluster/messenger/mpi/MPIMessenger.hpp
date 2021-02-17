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

class MPIMessenger : public Messenger {
private:
	bool _mpi_comm_data_raw = 0;

	// Default value useful for asserts
	int _wrank = -1, _wsize = -1;
	MPI_Comm INTRA_COMM, INTRA_COMM_DATA_RAW, PARENT_COMM;

	// Upper bound MPI tag supported by current implementation,
	// used for masking MPI tags to prevent out-of-range MPI
	// tags when sending/receiving large number of messages.
	int _mpi_ub_tag = 0;

	int createTag(const Message::Deliverable *delv) const
	{
		return _mpi_ub_tag & ((delv->header.id << 8) | delv->header.type);
	}

	int getTag(int messageId) const
	{
		return _mpi_ub_tag & ((messageId << 8) | DATA_RAW);
	}

	template<typename T>
	void testCompletionInternal(std::vector<T *> &pending);
public:

	MPIMessenger();
	~MPIMessenger();

	void sendMessage(Message *msg, ClusterNode const *toNode, bool block = false) override;

	void synchronizeAll(void) override;

	DataTransfer *sendData(
		const DataAccessRegion &region,
		const ClusterNode *toNode,
		int messageId, bool block,
		bool instrument) override;

	DataTransfer *fetchData(
		const DataAccessRegion &region,
		const ClusterNode *fromNode,
		int messageId,
		bool block,
		bool instrument) override;

	Message *checkMail() override;

	inline void testCompletion(std::vector<Message *> &pending) override
	{
		testCompletionInternal<Message>(pending);
	}

	inline void testCompletion(std::vector<DataTransfer *> &pending) override
	{
		testCompletionInternal<DataTransfer>(pending);
	}

	inline int getNodeIndex() const override
	{
		assert(_wrank >= 0);
		return _wrank;
	}

	inline int getMasterIndex() const override
	{
		return 0;
	}

	inline int getClusterSize() const override
	{
		assert(_wsize > 0);
		return _wsize;
	}

	inline bool isMasterNode() const override
	{
		assert(_wrank >= 0);
		return _wrank == 0;
	}
};

//! Register MPIMessenger with the object factory
namespace
{
	const bool __attribute__((unused))_registered_MPI_msn =
		Messenger::RegisterMSNClass<MPIMessenger>("mpi-2sided");
}

#endif /* MPI_MESSENGER_HPP */
