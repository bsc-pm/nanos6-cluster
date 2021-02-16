/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2019 Barcelona Supercomputing Center (BSC)
*/

#include "InstrumentCluster.hpp"
#include "InstrumentVerbose.hpp"

#include <Message.hpp>
#include <DataAccess.hpp>

using namespace Instrument::Verbose;

namespace Instrument {

	void clusterSendMessage(Message const *msg, int receiverId)
	{
		if (!_verboseClusterMessages) {
			return;
		}

		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent();

		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);

		logEntry->appendLocation(context);

		// If not receiverId then it is the end of the event.
		if (receiverId >= 0) {
			logEntry->_contents << " --> SendClusterMessage "
				<< msg->getName()
				<< " id:" << msg->getId() << " "
				<< msg->toString()
				<< " targetNode:" << receiverId;
		} else {
			logEntry->_contents << " <-- SendClusterMessage id:" << msg->getId();
		}

		addLogEntry(logEntry);
	}

	void clusterHandleMessage(Message const *msg, int senderId)
	{
		if (!_verboseClusterMessages) {
			return;
		}

		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent();

		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);

		logEntry->appendLocation(context);

		if (senderId >= 0) {
			logEntry->_contents << " --> HandleClusterMessage "
				<< msg->getName()
				<< " id:" << msg->getId() << " "
				<< msg->toString()
				<< " sourceNode:" << senderId;
		} else {
			logEntry->_contents << " <-- HandleClusterMessage id:" <<  msg->getId();
		}

		addLogEntry(logEntry);
	}

	void clusterDataSend(void *, size_t dataSize, int dest, int messageId, InstrumentationContext const &)
	{
		if(!_verboseClusterMessages) {
			return;
		}

		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent();

		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);
		assert(dest >= 0);

		logEntry->appendLocation(context);

		if(messageId >= 0) {
			logEntry->_contents << " --> SendingRawData size:"
				<< dataSize
				<< " targetNode:" << dest;
		} else {
			logEntry->_contents << " <-- SendingRawData id:" <<  messageId;
		}

		addLogEntry(logEntry);
	}

	void clusterDataReceived(void *, size_t dataSize, int source, int messageId, InstrumentationContext const &)
	{
		if(!_verboseClusterMessages) {
			return;
		}

		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent();

		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);
		assert(source >= 0);

		logEntry->appendLocation(context);

		if(messageId >= 0) {
			logEntry->_contents << " <-- ReceivingRawData size:"
				<< dataSize
				<< " sourceNode:" << source;
		} else {
			logEntry->_contents << " <-- ReceivingRawData id:" <<  messageId;
		}

		addLogEntry(logEntry);
	}	

	void taskIsOffloaded(task_id_t, InstrumentationContext const &)
	{
	}

	void stateNodeNamespace(int state, InstrumentationContext const &context)
	{
		std::string status;

		// TODO: This needs an enum probably. Now changes here imply changes in extrae version
		switch (state) {
			case 2:
				status = "Block";
				break;
			case 3:
				status = "Unblock";
				break;
			case 0:
				status = "Finish";
				break;
			case 1:
				status = "Init";
				break;
			default:
				status = "UNKNOWN!!";
		}

		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);

		logEntry->appendLocation(context);
		logEntry->_contents << status << " NodeNamespace task";

		addLogEntry(logEntry);
	}

	void emitClusterEvent(ClusterEventType, int, InstrumentationContext const &)
	{
	}

	void offloadedTaskCompletes(task_id_t, InstrumentationContext const &)
	{
	}

	void namespacePropagation(NamespacePropagation prop, DataAccessRegion region, InstrumentationContext const &)
	{
		// This can generate lots of output so normally disable it
		if (true) { // (!_verboseClusterMessages) {
			return;
		}

		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent();

		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);

		logEntry->appendLocation(context);

		logEntry->_contents << " Namespace propagation of " << region << ": ";
		switch(prop) {
			case NamespaceSuccessful:
				logEntry->_contents << " Successful";
				break;
			case NamespaceWrongPredecessor:
				logEntry->_contents << " Wrong predecessor at remote node";
				break;
			case NamespacePredecessorFinished:
				logEntry->_contents << " Predecessor already finished at remote node";
				break;
			case NamespaceNotHintedWithAncestor:
				logEntry->_contents << " Not hinted (ancestor present at remote node)";
				break;
			case NamespaceNotHintedNoPredecessor:
				logEntry->_contents << " Not hinted (no predecessor at remote node)";
				break;
			default:
				assert(false);
		}

		addLogEntry(logEntry);
	}
}
