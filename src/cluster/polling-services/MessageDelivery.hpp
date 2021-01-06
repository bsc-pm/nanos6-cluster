/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_DELIVERY_HPP
#define MESSAGE_DELIVERY_HPP

#include <atomic>
#include <vector>
#include <algorithm>

#include "lowlevel/PaddedSpinLock.hpp"
#include "system/ompss/SpawnFunction.hpp"
#include "tasks/Task.hpp"

#include "tasks/Task.hpp"
#include <Message.hpp>
#include <DataTransfer.hpp>
#include <ClusterManager.hpp>

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

	public:
		static void addPending(T *dt)
		{
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._incomingLock);
			_singleton._incomingPendings.push_back(dt);
		}

		static int takePendings()
		{
			// _lock must be taken
			int count = 0;
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._incomingLock);
			for (T *t : _singleton._incomingPendings) {
				_singleton._pendings.push_back(t);
				count ++;
			}
			_singleton._incomingPendings.clear();
			return count;
		}

		// Run a lambda function on the pending queue (with the lock taken)
		static bool checkPendingQueue(std::function<bool(T*)>  checkPending)
		{
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._lock);
			for (T *t: _singleton._pendings) {
				bool done = checkPending(t);
				if (done) {
					/* Return done flag */
					return true;
				}
			}
			/* Return not done */
			return false;
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

				pendings.erase(
					std::remove_if(
						pendings.begin(), pendings.end(),
						[](T *msg) {
							assert(msg != nullptr);

							const bool completed = msg->isCompleted();
							if (completed) {
								delete msg;
							}

							return completed;
						}
					),
					std::end(pendings)
				);
				firstIter = false;
			}
		}

		static void registerService()
		{
			assert(_singleton._live.load() == false);
			_singleton._live = true;
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
