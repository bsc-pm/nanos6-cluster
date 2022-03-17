/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_DELIVERY_HPP
#define MESSAGE_DELIVERY_HPP

#include <atomic>
#include <vector>

#include "InstrumentCluster.hpp"
#include "lowlevel/PaddedSpinLock.hpp"
#include "system/ompss/SpawnFunction.hpp"
#include "tasks/Task.hpp"

#include "tasks/Task.hpp"
#include <Message.hpp>
#include <DataTransfer.hpp>
#include <ClusterManager.hpp>
#include <ClusterUtil.hpp>

#define TIMEOUT 500

class Message;
class DataTransfer;

namespace ClusterPollingServices {

	template<typename T>
	class PendingQueue {

	private:
		PaddedSpinLock<> _incomingLock;
		std::vector<T *> _incomingPendings;
		PaddedSpinLock<> _lock;
		std::vector<T *> _pendings;
		std::atomic<bool> _live;

		static PendingQueue<T> _singleton;
		Instrument::ClusterEventType _eventTypeIncoming = Instrument::ClusterEventType::ClusterNoEvent;
		Instrument::ClusterEventType _eventTypePending = Instrument::ClusterEventType::ClusterNoEvent;
		Instrument::ClusterEventType _eventTypeBytes = Instrument::ClusterEventType::ClusterNoEvent;
		int _queueBytes;

	public:
		PendingQueue()
		{
			if (std::is_same<T, DataTransfer>::value) {
				_eventTypeIncoming = Instrument::ClusterEventType::PendingDataTransfersIncoming;
				_eventTypePending = Instrument::ClusterEventType::PendingDataTransfers;
				_eventTypeBytes = Instrument::ClusterEventType::PendingDataTransferBytes;
			}
		}

		static void addPendingVector(std::vector<T *> vdt)
		{
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._incomingLock);
			_singleton._incomingPendings.insert(
				_singleton._incomingPendings.end(), vdt.begin(), vdt.end());

			Instrument::emitClusterEvent(_singleton._eventTypeIncoming, _singleton._incomingPendings.size());
		}

		static void addPending(T * dt)
		{
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._incomingLock);
			_singleton._incomingPendings.push_back(dt);
			Instrument::emitClusterEvent(_singleton._eventTypeIncoming, _singleton._incomingPendings.size());
		}

		static int takePendings()
		{
			// _lock must be taken
			int count = 0;
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._incomingLock);
			for (T *t : _singleton._incomingPendings) {
				_singleton._pendings.push_back(t);
				count++;
				_singleton._queueBytes += (int)t->getSize();
			}
			_singleton._incomingPendings.clear();
			if (count > 0) {
				Instrument::emitClusterEvent(_singleton._eventTypeIncoming, 0);
				Instrument::emitClusterEvent(_singleton._eventTypePending, _singleton._pendings.size());
				Instrument::emitClusterEvent(_singleton._eventTypeBytes, _singleton._queueBytes);
			}
			return count;
		}

		// When the function returns false the service stops.
		static bool executeService()
		{
			// The service is already unregistered, so finish it.
			if (!_singleton._live.load()) {
				return false;
			}

			std::vector<T *> &pendings = _singleton._pendings;
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._lock);

			bool firstIter = true;
			while(1) {
				int count = takePendings();

				// There is nothing (new) to process so we can exit now.
				if (pendings.size() == 0 || (!firstIter && count == 0)) {
					return true;
				}

				ClusterManager::testCompletion<T>(pendings);

				int numCompleted = 0;
				pendings.erase(
					std::remove_if(
						pendings.begin(), pendings.end(),
						[&](T *msg) {
							assert(msg != nullptr);

							const bool completed = msg->isCompleted();
							if (completed) {
								numCompleted++;
								_singleton._queueBytes -= (int)msg->getSize();
								delete msg;
							}

							return completed;
						}
					),
					std::end(pendings)
				);

				if (numCompleted > 0) {
					Instrument::emitClusterEvent(_singleton._eventTypePending, pendings.size());
					Instrument::emitClusterEvent(_singleton._eventTypeBytes, _singleton._queueBytes);
				}
				firstIter = false;
			}
		}

		static void registerService()
		{
			assert(_singleton._live.load() == false);
			_singleton._live = true;
		}

		static void waitUntilFinished()
		{
			assert(_singleton._live.load() == true);
			bool done = false;
			while (!done) {
				{
					// Always take _lock before _incomingLock
					std::lock_guard<PaddedSpinLock<>> guard1(_singleton._lock);
					std::lock_guard<PaddedSpinLock<>> guard2(_singleton._incomingLock);
					done = _singleton._pendings.empty() && _singleton._incomingPendings.empty();
				}
				if (!done) {
					clusterCout << "Waiting for message delivery\n";
					sleep(1);
					executeService();
				}
			}
		}

		static void unregisterService()
		{
			assert(_singleton._live.load() == true);
			_singleton._live = false;

#ifndef NDEBUG
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._lock);
			assert(_singleton._pendings.empty());
#endif
		}
	};

	template <typename T> PendingQueue<T> PendingQueue<T>::_singleton;

}

#endif /* MESSAGE_DELIVERY_HPP */
