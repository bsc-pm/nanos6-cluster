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
		for(int j=0; j<TOTAL_MESSAGE_TYPES; ++j) {
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
		if (!start) {
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
			size_t totalNumberMessagesSent = 0;
			size_t totalNumberMessagesRecived = 0;
			size_t totalBytesSent = 0;
			size_t totalBytesRecived = 0;
			size_t totalNumberMessagess = 0;
			size_t totalBytes = 0;

			output << std::endl;
			for(int type=0; type < TOTAL_MESSAGE_TYPES; ++type) {
				output << "# STATS\t"
					<< std::left << std::setw(30) << MessageTypeStr[type] << std::setw(0) << std::right
					<< "\tsent msgs:\t" << countMessagesSent[type]
					<< "\tsent bytes:\t" << bytesMessagesSent[type]
					<< "\trcvd msgs:\t" << countMessagesReceived[type]
					<< "\trcvd bytes:\t" << bytesMessagesReceived[type] << std::endl;

				totalNumberMessagesSent += countMessagesSent[type];
				totalNumberMessagesRecived += countMessagesReceived[type];
				
				totalBytesSent += bytesMessagesSent[type];
				totalBytesRecived += bytesMessagesReceived[type];
			}
			totalNumberMessagess = totalNumberMessagesSent + totalNumberMessagesRecived;
			totalBytes = totalBytesSent + totalBytesRecived;
			
			output << std::endl;
			output << "# Messages Summary:" << std::endl;
			output << std::right << std::setw(10) << "# Total Messages\t" 
			<< std::left << std::setw(10) << totalNumberMessagess << std::endl;
			output << std::right << std::setw(10) << "\tSent\t" << 
			std::left << std::setw(10) << totalNumberMessagesSent << std::endl;
			output << std::right << std::setw(10) << "\tRecived\t" << 
			std::left << std::setw(10) << totalNumberMessagesRecived << std::endl;
			output << std::right << std::setw(10) << "# Total Bytes\t" << 
			std::left << std::setw(10) << totalBytes << std::endl;
			output << std::right << std::setw(10) << "\tSent\t" << 
			std::left << std::setw(10) << totalBytesSent << std::endl;
			output << std::right << std::setw(10) << "\tRecived\t" << 
			std::left << std::setw(10) << totalBytesRecived << std::endl;
		}
		
		output << std::endl;

		int namespaceCounterSum = 0;
		output << "# Namespace propagation by access:" << std::endl;
		for(int i=0; i< MaxNamespacePropagation; ++i) {
			output << "\t" << std::left << std::setw(30)  << namespaceNames[i] << "\t"
			<< std::setw(10) << std::right << namespaceCounter[i] << std::endl;
			namespaceCounterSum += namespaceCounter[i]; 
		}
		output << std::endl;
		if (namespaceCounterSum > 0) {
			for(int i=0; i< MaxNamespacePropagation; ++i) {
				output << "\t" << std::left << std::setw(30) << namespaceNames[i] << "\t" 
				<< std::fixed << std::setprecision(2) 
				<< std::setw(10) << std::right 
				<< (100.0 * namespaceCounter[i] / namespaceCounterSum) << "\%" 
				<< std::endl;
			}
		}
		output << std::endl;

		int dataFetchCounterSum = 0;
		output << "# Data fetches by access:" << std::endl;
		for(int i=0; i< MaxDataFetch; ++i) {
			output << "\t" << std::left << std::setw(30) << dataFetchNames[i] << "\t" 
			<< std::setw(10) << std::right << dataFetchCounter[i] << std::endl;
			dataFetchCounterSum += dataFetchCounter[i];
		}
		output << std::endl;
		if (dataFetchCounterSum > 0) {
			for(int i=0; i< MaxDataFetch; ++i) {
				output << "\t" << std::left << std::setw(30) << dataFetchNames[i] << "\t" 
				<< std::fixed << std::setprecision(2)
				<< std::setw(10) << std::right 
				<< (100.0 * dataFetchCounter[i] / dataFetchCounterSum) << "\%" << std::endl;
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
