/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_CLUSTER_HPP
#define INSTRUMENT_CLUSTER_HPP

#include <InstrumentInstrumentationContext.hpp>
#include <InstrumentThreadInstrumentationContext.hpp>
#include <DataAccessRegion.hpp>

class Message;

namespace Instrument {

	enum NamespacePropagation {
		NamespaceSuccessful = 0,           // Huccessful namespace propagation
		NamespaceWrongPredecessor,         // Hinted, but wrong predecessor in namespace
		NamespacePredecessorFinished,      // Hinted, but no predecessor in namespace (must have finished)
		NamespaceNotHintedWithAncestor,    // Not hinted with an ancestor present
		NamespaceNotHintedNoPredecessor,   // Not hinted, no predecessor present
		MaxNamespacePropagation
	};

	enum DataFetch {
		FetchRequired = 0,
		FoundInPending,
		EarlyWriteID,
		LateWriteID,
		MaxDataFetch
	};

	/* NOTE: this must match the order of the clusterEventType array */
	enum ClusterEventType {
		ClusterNoEvent = 0,
		OffloadedTasksWaiting,
		PendingDataTransfers,
		PendingDataTransferBytes,
		PendingDataTransfersIncoming,
		MaxClusterEventType
	};

	//! For hybrid MPI + Nanos6@cluster + DLB, this function is called
	//! when MPI_COMM_WORLD is split into appranks
	void summarizeSplit(
		int externalRank,
		int nodeNum,
		int apprankNum,
		InstrumentationContext const &context =
				ThreadInstrumentationContext::getCurrent()
	);

	//! This function is called when initiating a Message sending
	//!
	//! \param[in] msg is the Message we are sending
	//! \param[in] receiverId is the index of the receiver node.
	//! If this parameter is -1 then this means it is the end of an event.
	void clusterSendMessage(Message const *msg, int receiverId);

	//! This function is called when we start handling a received Message
	//!
	//! \param[in] msg is the Message we received and currently handling
	//! \param[in] senderId is the index of the sender node.
	//! If this parameter is -1 then this means it is the end of an event.
	void clusterHandleMessage(Message const *msg, int senderId);

	//! This function is called when sending raw data
	void clusterDataSend(
		void *address,
		size_t size,
		int dest,
		int messageId,
		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent()
	);

	//! This function is called when receiving raw data
	//!
	void clusterDataReceived(
		void *address,
		size_t size,
		int dest,
		int messageId,
		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent()
	);

	//! \brief Indicates that the task has been offloaded to another node
	//! \param[in] taskId the task identifier for the offloaded task
	void taskIsOffloaded(
		task_id_t taskId,
	    InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent()
	);

	void stateNodeNamespace(
		int state,
		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent()
	);

	void emitClusterEvent(
		ClusterEventType clusterEventType,
		int value,
		InstrumentationContext const & = ThreadInstrumentationContext::getCurrent()
	);

	void offloadedTaskCompletes(
		task_id_t taskId,
		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent()
	);

	void namespacePropagation(
		NamespacePropagation,
		DataAccessRegion region,
		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent()
	);

	void dataFetch(
		DataFetch,
		DataAccessRegion,
		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent()
	);
}

#endif //! INSTRUMENT_CLUSTER_HPP
