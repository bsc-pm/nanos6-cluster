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
	std::atomic<size_t> namespaceCounter[MaxNamespacePropagation];
	std::atomic<size_t> dataFetchCounter[MaxDataFetch];

	// Must match definition of enum NamespacePropagation
	const char *namespaceNames[MaxNamespacePropagation] =
	{
		"Successful",
		"Wrong predecessor",
		"Predecessor finished",
		"Not hinted ancestor present",
		"Not hinted no ancestor"
	};

	// Must match definition of enum DataFetch
	const char *dataFetchNames[MaxDataFetch] =
	{
		"Fetch required",
		"Found in pending",
		"Early Write ID",
		"Late Write ID"
	};

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
		bytesMessagesSent[messageType] += msg->getSize() + sizeof(Message::msg_header);
	}

	void clusterHandleMessage(size_t n, Message **msgs, int start)
	{
		if (senderId < 0) {
			return;
		}
		for (size_t i = 0; i < n; ++i) {
			size_t messageType = static_cast<size_t>(msgs[i]->getType());
			assert(messageType < TOTAL_MESSAGE_TYPES);
			countMessagesReceived[messageType] ++;
			bytesMessagesReceived[messageType] += msgs[i]->getSize() + sizeof(Message::msg_header);
		}
	}

	void clusterDataSend(void *, size_t size, int, int messageId)
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
		int namespaceCounterSum = 0;
		output << "Namespace propagation by access: ";
		for(int i=0; i< MaxNamespacePropagation; i++) {
			output << namespaceNames[i] << ": " << namespaceCounter[i] << " ";
			namespaceCounterSum += namespaceCounter[i];
		}
		output << std::endl;
		if (namespaceCounterSum > 0) {
			for(int i=0; i< MaxNamespacePropagation; i++) {
				output << namespaceNames[i] << ": " << std::fixed << std::setprecision(2)
				       << (100.0 * namespaceCounter[i] / namespaceCounterSum) << "\% ";
			}
		}
		output << std::endl;

		int dataFetchCounterSum = 0;
		output << "Data fetches by access: ";
		for(int i=0; i< MaxDataFetch; i++) {
			output << dataFetchNames[i] << ": " << dataFetchCounter[i] << " ";
			dataFetchCounterSum += dataFetchCounter[i];
		}
		output << std::endl;
		if (dataFetchCounterSum > 0) {
			for(int i=0; i< MaxDataFetch; i++) {
				output << dataFetchNames[i] << ": " << std::fixed << std::setprecision(2)
				       << (100.0 * dataFetchCounter[i] / dataFetchCounterSum) << "\% ";
			}
		}
		output << std::endl;
	}

	void namespacePropagation(NamespacePropagation prop, DataAccessRegion, InstrumentationContext const &)
	{
		assert(prop < MaxNamespacePropagation);
		namespaceCounter[prop] += 1;
	}

	void dataFetch(DataFetch df, DataAccessRegion, InstrumentationContext const &)
	{
		assert(df < MaxDataFetch);
		dataFetchCounter[df] += 1;
	}
}
