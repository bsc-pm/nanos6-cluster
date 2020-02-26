/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2019 Barcelona Supercomputing Center (BSC)
*/

#include "InstrumentCluster.hpp"
#include "InstrumentVerbose.hpp"

#include <Message.hpp>

using namespace Instrument::Verbose;

namespace Instrument {
	void clusterMessageInitSend(Message const *msg, int receiverId,
		InstrumentationContext const &context)
	{
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

	void clusterMessageCompleteSend(Message const *msg,
		InstrumentationContext const &context)
	{
		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);

		logEntry->appendLocation(context);
		logEntry->_contents <<
			" <-- SendClusterMessage id:" << msg->getId();
	}

	void enterHandleReceivedMessage(Message const *msg, int senderId,
		InstrumentationContext const &context)
	{
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

	void exitHandleReceivedMessage(Message const *msg,
		InstrumentationContext const &context)
	{
		LogEntry *logEntry = getLogEntry(context);
		assert(logEntry != nullptr);

		logEntry->appendLocation(context);
		logEntry->_contents <<
			" <-- HandleClusterMessage id:" << msg->getId();

		addLogEntry(logEntry);
	}
}
