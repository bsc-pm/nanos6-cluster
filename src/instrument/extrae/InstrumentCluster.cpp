/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2019 Barcelona Supercomputing Center (BSC)
*/

#include <atomic>
#include "InstrumentCluster.hpp"
#include "InstrumentExtrae.hpp"

#include <Message.hpp>

#define CLUSTER_EVENTS 3
#define EVENT_PREFIX_SIZE 16

namespace Instrument {
	/* NOTE: this must match the order of HybridClusterEventType */
	static extrae_type_t clusterEventTypeToExtraeType[MaxClusterEventType] = {
		(extrae_type_t) -1,
		(extrae_type_t) EventType::OFFLOADED_TASKS_WAITING,
		(extrae_type_t) EventType::PENDING_DATA_TRANSFERS,
		(extrae_type_t) EventType::PENDING_DATA_TRANSFER_BYTES,
		(extrae_type_t) EventType::PENDING_DATA_TRANSFERS_INCOMING,
	};

	static const char *clusterEventTypeToName[MaxClusterEventType] = {
		"No event",
		"Number of unfinished offloaded tasks",
		"Number of data transfers being waited for",
		"Total bytes of data transfers being waited for",
		"Number of data transfers queued to wait for"
	};

	static std::atomic<int> _totalOffloadedTasksWaiting;

	void defineClusterExtraeEvents()
	{
		if (!Extrae::_extraeInstrumentCluster)
			return;

		//! Event variables
		const char evtStr[CLUSTER_EVENTS][EVENT_PREFIX_SIZE] = { "Send ", "Receive ", "Handle "};

		const extrae_type_t extraeType[CLUSTER_EVENTS] = {
			(extrae_type_t) EventType::MESSAGE_SEND,
			(extrae_type_t) EventType::MESSAGE_RECEIVE,
			(extrae_type_t) EventType::MESSAGE_HANDLE
		};

		/* Cluster message events */
		const size_t totalTypes = (size_t)TOTAL_MESSAGE_TYPES;
		extrae_value_t values[totalTypes];

		char *valueStr[totalTypes];
		for (size_t i = 0; i < totalTypes; ++i) {
			valueStr[i] = (char *) alloca((EVENT_PREFIX_SIZE + MSG_NAMELEN + 1) * sizeof(char));
		}

		for (size_t event = 0; event < CLUSTER_EVENTS; ++event) {
			for (size_t i = 0; i < totalTypes; ++i) {
				strncpy(valueStr[i], evtStr[event], EVENT_PREFIX_SIZE);
				strncat(valueStr[i], MessageTypeStr[i], MSG_NAMELEN);

				values[i] = (extrae_value_t)(i + 1);
			}

			char typeStr[32] = "Message ";
			strncat(typeStr, evtStr[event], EVENT_PREFIX_SIZE);

			ExtraeAPI::define_event_type(extraeType[event], typeStr, totalTypes, values, valueStr);
		}

		/* Other cluster events */
		for (size_t event = 0; event < MaxClusterEventType; ++event) {
			ExtraeAPI::define_event_type(
				clusterEventTypeToExtraeType[event], clusterEventTypeToName[event], 0, nullptr, nullptr);
		}
	}

	void clusterSendMessage(Message const *msg, int receiver)
	{
		if (!Extrae::_extraeInstrumentCluster)
			return;

		const unsigned int messageType = msg->getType();

		extrae_combined_events_t ce;
		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;

		extrae_type_t type = (extrae_type_t) EventType::MESSAGE_SEND;
		ce.Types = &type;

		// Default values.
		ce.nEvents = 1;
		extrae_value_t value = 0;
		ce.Values = &value;

		ce.nCommunications = 0;
		ce.Communications = NULL;

		extrae_user_communication_t com;

		if (receiver >= 0) {
			com.type = EXTRAE_USER_SEND;
			com.tag = (extrae_comm_tag_t)EventType::MESSAGE_SEND;
			com.size = messageType;
			com.partner = receiver;
			com.id = msg->getId();

			value = (extrae_value_t)(messageType + 1);
			ce.Values = &value;
			ce.nCommunications = 1;
			ce.Communications = &com;
		}

		Extrae::emit_CombinedEvents(&ce);
	}

	void clusterReceiveMessage(int msgType, Message const *msg)
	{
		if (!Extrae::_extraeInstrumentCluster)
			return;

		extrae_combined_events_t ce;
		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;

		extrae_type_t type = (extrae_type_t)EventType::MESSAGE_RECEIVE;
		ce.Types = &type;

		// Default values.
		ce.nEvents = 1;
		extrae_value_t value = 0;
		ce.Values = &value;

		ce.nCommunications = 0;
		ce.Communications = NULL;

		if (msg == nullptr) {
			// msg is null BEFORE MPI_Recv, when we start this event.
			value = (extrae_value_t)(msgType + 1);
			ce.Values = &value;
		} else {
			// We finish the communication AFTER the MPI_Recv; when we have a msg already here,
			// because we need some info to finalize the communication correctly because the tag
			// doesn't contain all the information.

			ce.nCommunications = 2;
			ce.Communications =
				(extrae_user_communication_t *) alloca(2 * sizeof(extrae_user_communication_t));

			// End point to the send counterpart message
			ce.Communications[0].type = EXTRAE_USER_RECV;
			ce.Communications[0].tag = (extrae_comm_tag_t)EventType::MESSAGE_SEND;
			ce.Communications[0].size = msg->getType();
			ce.Communications[0].partner = msg->getSenderId();
			ce.Communications[0].id = msg->getId();

			ce.Communications[1].type = EXTRAE_USER_SEND;
			ce.Communications[1].tag = (extrae_comm_tag_t)EventType::MESSAGE_RECEIVE;
			ce.Communications[1].size = msg->getType();
			ce.Communications[1].partner = EXTRAE_COMM_PARTNER_MYSELF;
			ce.Communications[1].id = msg->getId();
		}

		Extrae::emit_CombinedEvents(&ce);
	}

	void clusterHandleMessage(size_t n, Message **msgs, int start)
	{
		if (!Extrae::_extraeInstrumentCluster)
			return;

		extrae_combined_events_t ce;
		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;

		extrae_type_t type = (extrae_type_t)EventType::MESSAGE_HANDLE;
		ce.Types = &type;

		// Default values.
		ce.nEvents = 1;
		extrae_value_t value = 0;
		ce.Values = &value;

		ce.nCommunications = 0;
		ce.Communications = NULL;

		if (start != 0) {
			// We asume all of them are the same type
			ce.Values[0] = (extrae_value_t)(msgs[0]->getType() + 1);;

			// As we handle multiple messages together we need to terminate all the communications
			// started in the clusterReceiveMessage event.
			ce.nCommunications = n;
			ce.Communications =
				(extrae_user_communication_t *) alloca(n * sizeof(extrae_user_communication_t));
			assert(ce.Communications != nullptr);

			for (size_t i = 0; i < n; ++i) {
				ce.Communications[i].type = EXTRAE_USER_RECV;
				ce.Communications[i].tag = (extrae_comm_tag_t)EventType::MESSAGE_RECEIVE;
				ce.Communications[i].size = msgs[i]->getType();
				ce.Communications[i].partner = EXTRAE_COMM_PARTNER_MYSELF;
				ce.Communications[i].id = msgs[i]->getId();
			}
		}

		Extrae::emit_CombinedEvents(&ce);
	}

	//! This function is called when sending raw data
	void clusterDataSend(void *, size_t, int dest, int messageId)
	{
		if (!Extrae::_extraeInstrumentCluster)
		return;

		const unsigned int messageType = MessageType::DATA_RAW;

		extrae_combined_events_t ce;
		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;

		extrae_type_t type = (extrae_type_t) EventType::MESSAGE_SEND;
		ce.Types = &type;

		ce.nEvents = 1;
		extrae_value_t value = 0;
		ce.Values = &value;

		ce.nCommunications = 0;
		ce.Communications = NULL;

		if (messageId >= 0) {
			value = (extrae_value_t)(messageType + 1);
			ce.Values = &value;
			ce.nCommunications = 1;
			ce.Communications =
				(extrae_user_communication_t *) alloca(sizeof(extrae_user_communication_t));

			ce.Communications[0].type = EXTRAE_USER_SEND;
			ce.Communications[0].tag = (extrae_comm_tag_t)EventType::MESSAGE_SEND;
			ce.Communications[0].size = messageType;
			ce.Communications[0].partner = dest;

			// NOTE: this assumes that the message ID is globally unique (i.e. you
			// cannot receive a MessageDmalloc and MessageDataFetch from the same
			// node with the same messageId, as they would both result in a DATA_RAW
			// message with the same messageId.
			ce.Communications[0].id = messageId;
		}

		Extrae::emit_CombinedEvents(&ce);
	}

	//! This function is called when receiving raw data
	void clusterDataReceived(void *, size_t, int source, int messageId, InstrumentationContext const &)
	{
		if (!Extrae::_extraeInstrumentCluster)
		return;

		const unsigned int messageType = MessageType::DATA_RAW;

		extrae_combined_events_t ce;
		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;

		extrae_type_t type = (extrae_type_t) EventType::MESSAGE_HANDLE;
		ce.Types = &type;

		ce.nEvents = 1;
		extrae_value_t value = 0;
		ce.Values = &value;

		ce.nCommunications = 0;
		ce.Communications = NULL;

		if (messageId >= 0) {
			value = (extrae_value_t)(messageType + 1);
			ce.Values = &value;
			ce.nCommunications = 1;
			ce.Communications =
				(extrae_user_communication_t *) alloca(sizeof(extrae_user_communication_t));
			ce.Communications[0].type = EXTRAE_USER_RECV;
			ce.Communications[0].tag = (extrae_comm_tag_t)EventType::MESSAGE_SEND;
			ce.Communications[0].size = messageType;
			ce.Communications[0].partner = source;

			// NOTE: this assumes that the message ID is globally unique (i.e. you
			// cannot have a MessageDmalloc and MessageDataFetch with the same messageId, as
			// both have accompanying DATA_RAW messages).
			ce.Communications[0].id = messageId;
		}

		Extrae::emit_CombinedEvents(&ce);
	}

	void taskIsOffloaded(__attribute__((unused)) task_id_t taskId,
		__attribute__((unused)) InstrumentationContext const &context) {
		// Do not add an event for now, but decrement _readyTasks
		_readyTasks--;
		Instrument::emitClusterEvent(
			ClusterEventType::OffloadedTasksWaiting, ++_totalOffloadedTasksWaiting
		);
	}

	void emitClusterEvent(
		ClusterEventType clusterEventType,
		int eventValue,
		InstrumentationContext const &
	) {
		if (!Extrae::_extraeInstrumentCluster || clusterEventType == ClusterNoEvent)
			return;

		extrae_type_t type = clusterEventTypeToExtraeType[clusterEventType];

		Extrae::emit_SimpleEvent(type, (extrae_value_t) eventValue);
	}

	void stateNodeNamespace(int state, InstrumentationContext const &)
	{
		if (!Extrae::_extraeInstrumentCluster)
			return;

		extrae_type_t type = (extrae_type_t)EventType::NODE_NAMESPACE;

		// TODO: This needs an enum probably. Now changes here imply changes in verbose version
		// 1:Init && 3:Unblock
		// 0:Fini && 2:Block
		extrae_value_t value = state % 2;

		extrae_combined_events_t ce;
		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;
		ce.Communications = NULL;
		ce.Types = &type;
		ce.Values = &value;

		Extrae::emit_CombinedEvents(&ce);
	}

	void offloadedTaskCompletes(task_id_t, InstrumentationContext const &context)
	{
		Instrument::emitClusterEvent(ClusterEventType::OffloadedTasksWaiting, --_totalOffloadedTasksWaiting, context);
	}

	void MPILock()
	{
		Instrument::Extrae::_lockMPI.lock();
	}

	void MPIUnLock()
	{
		Instrument::Extrae::_lockMPI.unlock();
	}
}
