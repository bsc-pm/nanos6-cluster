/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2019 Barcelona Supercomputing Center (BSC)
*/

#include "InstrumentCluster.hpp"
#include "InstrumentVerbose.hpp"

#include <Message.hpp>

using namespace Instrument::Verbose;

namespace Instrument {

	void clusterMessageInitSend(
		Message const *msg,
		int receiverId,
		InstrumentationContext const &context
	) {
		if (!_verboseClusterMessages) {
			return;
		}

		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);

		logEntry->appendLocation(context);
		logEntry->_contents << " --> SendClusterMessage "
			<< msg->getName()
			<< " id:" << msg->getId() << " "
			<< msg->toString()
			<< " targetNode:" << receiverId;

		addLogEntry(logEntry);
	}

	void clusterMessageCompleteSend(Message const *msg, InstrumentationContext const &context)
	{
		if (!_verboseClusterMessages) {
			return;
		}

		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);

		logEntry->appendLocation(context);
		logEntry->_contents << " <-- SendClusterMessage id:" << msg->getId();

		addLogEntry(logEntry);
	}

	void enterHandleReceivedMessage(
		Message const *msg,
		int senderId,
		InstrumentationContext const &context
	) {
		if (!_verboseClusterMessages) {
			return;
		}

		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);

		logEntry->appendLocation(context);
		logEntry->_contents << " --> HandleClusterMessage "
			<< msg->getName()
			<< " id:" << msg->getId() << " "
			<< msg->toString()
			<< " sourceNode:" << senderId;

		addLogEntry(logEntry);
	}

	void exitHandleReceivedMessage(Message const *msg, InstrumentationContext const &context)
	{
		if (!_verboseClusterMessages) {
			return;
		}

		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);

		logEntry->appendLocation(context);
		logEntry->_contents << " <-- HandleClusterMessage id:" <<  msg->getId();

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


}
