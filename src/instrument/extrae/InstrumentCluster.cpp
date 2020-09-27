/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2019 Barcelona Supercomputing Center (BSC)
*/

#include <atomic>
#include "InstrumentCluster.hpp"
#include "InstrumentExtrae.hpp"

#include <Message.hpp>

#define CLUSTER_EVENTS 2
#define EVENT_PREFIX_SIZE 8

namespace Instrument {

	/* NOTE: this must match the order of HybridClusterEventType */
	static extrae_type_t clusterEventTypeToExtraeType[MaxClusterEventType] = {
		(extrae_type_t) EventType::OFFLOADED_TASKS_WAITING,
		(extrae_type_t) EventType::PENDING_DATA_TRANSFERS,
		(extrae_type_t) EventType::PENDING_DATA_TRANSFER_BYTES,
		(extrae_type_t) EventType::PENDING_DATA_TRANSFERS_INCOMING,
	};

	static const char *clusterEventTypeToName[MaxClusterEventType] = {
		"Number of unfinished offloaded tasks",
		"Number of data transfers being waited for",
		"Total bytes of data transfers being waited for",
		"Number of data transfers queued to wait for"
	};

	static std::atomic<int> _totalOffloadedTasksWaiting;

	void defineClusterExtraeEvents()
	{
		if (!_extraeInstrumentCluster)
			return;

		//! Event variables
		const char evtStr[CLUSTER_EVENTS][EVENT_PREFIX_SIZE] = { "Send ", "Handle "};

		extrae_type_t extraeType[CLUSTER_EVENTS] = {
			(extrae_type_t) EventType::MESSAGE_SEND,
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

	void clusterMessageInitSend(Message const *msg, int receiver, InstrumentationContext const &)
	{
		if (!_extraeInstrumentCluster)
			return;

		const unsigned int messageType = msg->getType();
		extrae_type_t type = (extrae_type_t) EventType::MESSAGE_SEND;
		extrae_value_t value = (extrae_value_t)(messageType + 1);

		extrae_combined_events_t ce;
		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 1;
		ce.Communications =
			(extrae_user_communication_t *) alloca(sizeof(extrae_user_communication_t));
		ce.Types = &type;
		ce.Values = &value;

		ce.Communications[0].type = EXTRAE_USER_SEND;
		ce.Communications[0].tag = (extrae_comm_tag_t)EventType::MESSAGE_SEND;
		ce.Communications[0].size = messageType;
		ce.Communications[0].partner = receiver;
		ce.Communications[0].id = msg->getId();

		ExtraeAPI::emit_CombinedEvents(&ce);
	}

	void clusterMessageCompleteSend(Message const *, InstrumentationContext const &)
	{
		if (!_extraeInstrumentCluster)
			return;

		extrae_type_t type = (extrae_type_t) EventType::MESSAGE_SEND;
		extrae_value_t value = 0;

		extrae_combined_events_t ce;
		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;
		ce.Communications = NULL;
		ce.Types = &type;
		ce.Values = &value;

		ExtraeAPI::emit_CombinedEvents(&ce);
	}

	void enterHandleReceivedMessage(Message const *msg, int senderId, InstrumentationContext const &)
	{
		if (!_extraeInstrumentCluster)
			return;

		const unsigned int messageType = msg->getType();
		extrae_type_t type = (extrae_type_t)EventType::MESSAGE_HANDLE;
		extrae_value_t value = (extrae_value_t)(messageType + 1);

		extrae_combined_events_t ce;
		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 1;
		ce.Communications =
			(extrae_user_communication_t *) alloca(sizeof(extrae_user_communication_t));
		ce.Types = &type;
		ce.Values = &value;

		ce.Communications[0].type = EXTRAE_USER_RECV;
		ce.Communications[0].tag = (extrae_comm_tag_t)EventType::MESSAGE_SEND;
		ce.Communications[0].size = messageType;
		ce.Communications[0].partner = senderId;
		ce.Communications[0].id = msg->getId();

		ExtraeAPI::emit_CombinedEvents(&ce);
	}

	void exitHandleReceivedMessage(Message const *, InstrumentationContext const &)
	{
		if (!_extraeInstrumentCluster)
			return;

		extrae_type_t type = (extrae_type_t)EventType::MESSAGE_HANDLE;
		extrae_value_t value = 0;

		extrae_combined_events_t ce;
		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;
		ce.Communications = NULL;
		ce.Types = &type;
		ce.Values = &value;

		ExtraeAPI::emit_CombinedEvents(&ce);
	}

	void taskIsOffloaded(__attribute__((unused)) task_id_t taskId,
		__attribute__((unused)) InstrumentationContext const &context) {
		// Do not add an event for now, but decrement _readyTasks
		_readyTasks--;
		Instrument::emitClusterEvent(ClusterEventType::OffloadedTasksWaiting, ++_totalOffloadedTasksWaiting);
	}

	void emitClusterEvent(ClusterEventType clusterEventType, int eventValue, InstrumentationContext const &)
	{
		if (!_extraeInstrumentCluster)
			return;

		extrae_type_t type = clusterEventTypeToExtraeType[clusterEventType];
		extrae_value_t value = eventValue;

		extrae_combined_events_t ce;
		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;
		ce.Communications = NULL;
		ce.Types = &type;
		ce.Values = &value;

		ExtraeAPI::emit_CombinedEvents(&ce);
	}

	void stateNodeNamespace(int state, InstrumentationContext const &)
	{
		if (!_extraeInstrumentCluster)
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

		ExtraeAPI::emit_CombinedEvents(&ce);
	}

	void offloadedTaskCompletes(task_id_t, InstrumentationContext const &context)
	{
		Instrument::emitClusterEvent(ClusterEventType::OffloadedTasksWaiting, --_totalOffloadedTasksWaiting, context);
	}
}
