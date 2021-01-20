/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_DELIVERY_HPP
#define MESSAGE_DELIVERY_HPP

#include <atomic>
#include <vector>
#include <algorithm>

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
		Instrument::ClusterEventType _eventTypeIncoming;
		Instrument::ClusterEventType _eventTypePending;
		Instrument::ClusterEventType _eventTypeBytes;
		int _queueBytes;

	public:
		static void setEventTypes(Instrument::ClusterEventType eventTypeIncoming,
		                          Instrument::ClusterEventType eventTypePending,
								  Instrument::ClusterEventType eventTypeBytes) {
			_singleton._eventTypeIncoming = eventTypeIncoming;
			_singleton._eventTypePending = eventTypePending;
			_singleton._eventTypeBytes = eventTypeBytes;
		}

		static void addPending(T *dt)
		{
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._incomingLock);
			_singleton._incomingPendings.push_back(dt);
			if (_singleton._eventTypeIncoming) {
				Instrument::emitClusterEvent(_singleton._eventTypeIncoming, _singleton._incomingPendings.size());
			}
		}

		static int takePendings()
		{
			// _lock must be taken
			int count = 0;
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._incomingLock);
			for (T *t : _singleton._incomingPendings) {
				_singleton._pendings.push_back(t);
				count ++;
				_singleton._queueBytes += (int)t->getSize();
			}
			_singleton._incomingPendings.clear();
			if (count > 0 && _singleton._eventTypeIncoming) {
				Instrument::emitClusterEvent(_singleton._eventTypeIncoming, 0);
				Instrument::emitClusterEvent(_singleton._eventTypePending, _singleton._pendings.size());
				Instrument::emitClusterEvent(_singleton._eventTypeBytes, _singleton._queueBytes);
			}
			return count;
		}

		// Run a lambda function on the pending queue (with the lock taken)
		static bool checkPendingQueueInternal(std::function<bool(T*)>  checkPending,
		                                      PaddedSpinLock<> &lock,
											  std::vector<T *> &queue)
		{
			std::lock_guard<PaddedSpinLock<>> guard(lock);
			for (T *t: queue) {
				bool done = checkPending(t);
				if (done) {
					/* Return done flag */
					return true;
				}
			}
			/* Return not done */
			return false;
		}

		static bool checkPendingQueue(std::function<bool(T*)>  checkPending)
		{
			bool ret = checkPendingQueueInternal(checkPending, _singleton._incomingLock, _singleton._incomingPendings);
			if (!ret) {
				ret = checkPendingQueueInternal(checkPending, _singleton._lock, _singleton._pendings);
			}
			return ret;
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
				if (pendings.size() == 0
					|| (!firstIter && count == 0)) {
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

				if (numCompleted > 0 && _singleton._eventTypeIncoming) {
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
				std::lock_guard<PaddedSpinLock<>> guard1(_singleton._lock); // Always take _lock before _incomingLock
				std::lock_guard<PaddedSpinLock<>> guard2(_singleton._incomingLock);
				done = _singleton._pendings.empty() && _singleton._incomingPendings.empty();
				if (!done) {
					clusterCout << "Waiting for message delivery\n";
					sleep(1);
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
