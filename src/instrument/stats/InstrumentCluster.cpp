/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.
	Copyright (C) 2018-2019 Barcelona Supercomputing Center (BSC)
*/

#include <fstream>
#include <iomanip>

#include "ClusterManager.hpp"
#include "InstrumentCluster.hpp"

#include <Message.hpp>
#include <atomic>

namespace Instrument {
	std::atomic<size_t> countMessagesSent[TOTAL_MESSAGE_TYPES];
	std::atomic<size_t> countMessagesReceived[TOTAL_MESSAGE_TYPES];
	std::atomic<size_t> bytesMessagesSent[TOTAL_MESSAGE_TYPES];
	std::atomic<size_t> bytesMessagesReceived[TOTAL_MESSAGE_TYPES];

	void initClusterCounters()
	{
		for(int j=0; j<TOTAL_MESSAGE_TYPES; j++) {
			countMessagesSent[j] = 0;
			countMessagesReceived[j] = 0;
			bytesMessagesSent[j] = 0;
			bytesMessagesReceived[j] = 0;
		}
	}

	void clusterSendMessage(Message const *msg, int receiver)
	{
		if (receiver < 0) {
			return;
		}
		size_t messageType = static_cast<size_t>(msg->getType());
		assert(messageType < TOTAL_MESSAGE_TYPES);
		countMessagesSent[messageType] ++;
		bytesMessagesSent[messageType] += msg->getSize();
	}

	void clusterHandleMessage(Message const *msg, int senderId)
	{
		if (senderId < 0) {
			return;
		}
		size_t messageType = static_cast<size_t>(msg->getType());
		assert(messageType < TOTAL_MESSAGE_TYPES);
		countMessagesReceived[messageType] ++;
		bytesMessagesReceived[messageType] += msg->getSize();
	}

	void clusterDataSend(void *, size_t size, int, int messageId, InstrumentationContext const &)
	{
		if (messageId >=0 ) {
			// Only count entering data send not leaving it
			countMessagesSent[DATA_RAW] ++;
			bytesMessagesSent[DATA_RAW] += size;
		}
	}


	void clusterDataReceived(void *, size_t size, int, int messageId, InstrumentationContext const &)
	{
		if (messageId >=0 ) {
			// Only count entering data receive not leaving it
			countMessagesReceived[DATA_RAW] ++;
			bytesMessagesReceived[DATA_RAW] += size;
		}
	}

	void showClusterCounters(std::ofstream &output)
	{
		if (ClusterManager::inClusterMode()) {
			output << std::endl;
			for(int type=0; type < TOTAL_MESSAGE_TYPES; type++) {
				output << "STATS\t"
					<< std::left << std::setw(15) << MessageTypeStr[type] << std::setw(0) << std::right
					<< "\tsent msgs:\t" << countMessagesSent[type]
					<< "\tsent bytes:\t" << bytesMessagesSent[type]
					<< "\trcvd msgs:\t" << countMessagesReceived[type]
					<< "\trcvd bytes:\t" << bytesMessagesReceived[type] << std::endl;
			}
		}
	}
}
