/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_HANDLER_HPP
#define MESSAGE_HANDLER_HPP

#include <deque>
#include <unordered_map>
#include "lowlevel/PaddedSpinLock.hpp"

#include <ClusterManager.hpp>
#include <InstrumentCluster.hpp>
#include <InstrumentThreadInstrumentationContext.hpp>
#include <Message.hpp>
#include <MessageReleaseAccess.hpp>

class Task;

namespace ClusterPollingServices {

	template<typename T>
	class MessageHandler {
	private:
		static MessageHandler<T> _singleton;

		PaddedSpinLock<> _lock;
		std::atomic<bool> _live;

		// Number of worker tasks (in addition to this polling service)
		int _numWorkers;

		// Current number of stolen messages
		int _numStolen;

		// Messages that can be stolen by other workers
		std::deque<T*> _stealableMessages;

		// Messages that cannot be stolen (and that have lower priority)
		std::deque<T*> _nonStealableMessages;

		// For a given task, keep track of number of RELEASE_ACCESS messages
		// left to handle plus the TASK_FINISHED/RELEASE_ACCESS_AND_FINISH
		// message, if already received.
		struct WaitingTaskInfo {
			int _numReleaseAccesses;
			Message *_waitingTaskFinished;
		};

		std::unordered_map<Task *, WaitingTaskInfo> _waitingTaskInfo;

		// Queue a received message
		void queueMessage(T *msg)
		{
			std::lock_guard<PaddedSpinLock<>> guard(_lock);
			Task *task;

			switch(msg->getType()) {
				case DMALLOC:
				case DFREE:
				case DATA_FETCH:
				case DATA_SEND: // Not sure whether worth offloading to workers as just an MPI call
				case SATISFIABILITY:
				case NO_EAGER_SEND:
					// These messages have no ordering constraints and are worth handling
					// by other workers. All involve dependency system operations, which may
					// potentially be a lot of work. So put the message into _stealableMessages
					_stealableMessages.push_back(msg);
					break;

				case TASK_NEW:
				case SYS_FINISH:
					// These messages are handled by the polling service itself.
					// * TASK_NEW is too trivial: as it only requires queuing the task to be
					// created and submitted by the namespace
					// * SYS_FINISH must be the last message of all to be handled
					_nonStealableMessages.push_back(msg);
					break;

				case RELEASE_ACCESS:
					// This message is worth handling by other workers, but we need to ensure it
					// is done before handling the task's TASK_FINISHED. Increment the number of
					// RELEASE_ACCESSES for this task.
					task = dynamic_cast<MessageReleaseAccess *>(msg)->getTask();
					{
						auto it = _waitingTaskInfo.find(task);
						if (it == _waitingTaskInfo.end()) {
							_waitingTaskInfo[task] = WaitingTaskInfo({1, nullptr});
						} else {
							it->second._numReleaseAccesses++;
						}
						_stealableMessages.push_back(msg);
					}
					break;

				case TASK_FINISHED:
				case RELEASE_ACCESS_AND_FINISH:
					task = dynamic_cast<MessageReleaseAccess *>(msg)->getTask();
					{
						auto it = _waitingTaskInfo.find(task);
						if (it == _waitingTaskInfo.end()) {
							// No prior RELEASE_ACCESS messages, so it can be handled immediately
							_stealableMessages.push_back(msg);
						} else {
							// There are some RELEASE_ACCESS messages left to be handled first
							assert(it->second._waitingTaskFinished == nullptr);
							it->second._waitingTaskFinished = msg;
						}
					}
					break;

				case DATA_RAW:
				case TOTAL_MESSAGE_TYPES:
					// We should never see these messages
					assert(0);
			}
		}

		// Steal a message
		T *stealMessageInternal(__attribute__((unused)) bool isMessageHandlerItself)
		{
			std::lock_guard<PaddedSpinLock<>> guard(_lock);

			// Try to take a stealable task
			Message *msg = nullptr;
			if (_stealableMessages.size() > 0) {
				msg = _stealableMessages.front();
				_stealableMessages.pop_front();
			}

			// If not, and this is the polling service itself, take a non-stealable one
			if (!msg && isMessageHandlerItself) {
				if (_nonStealableMessages.size() > 0) {
					msg = _nonStealableMessages.front();
					if (msg->getType() == SYS_FINISH) {
						if (_numStolen > 0) {
							// Don't handle SYS_FINISH until all other messages
							// have been handled.
							return nullptr;
						}
					}
					_nonStealableMessages.pop_front();
				}
			}
			if (msg) {
				Instrument::clusterHandleMessage(msg, msg->getSenderId());
				_numStolen++;
			}
			return msg;
		}

		// Finished handling a Message
		void notifyDoneInternal(T *msg, __attribute__((unused)) bool isMessageHandlerItself)
		{
			Instrument::clusterHandleMessage(msg, -1);
			std::lock_guard<PaddedSpinLock<>> guard(_lock);
			_numStolen--;
			Task *task = nullptr;
			switch (msg->getType()) {
				case SYS_FINISH:
				case DMALLOC:
				case DFREE:
				case DATA_FETCH:
				case DATA_SEND:
				case TASK_NEW:
				case SATISFIABILITY:
				case NO_EAGER_SEND:
					// These messages have no ordering constraints, so do nothing
					break;

				case RELEASE_ACCESS:
					// After handling RELEASE_ACCESS, decrement the number of
					// RELEASE_ACCESS messages that must be done before finishing
					// the task
					task = dynamic_cast<MessageReleaseAccess *>(msg)->getTask();
					{
						auto it = _waitingTaskInfo.find(task);
						assert(it != _waitingTaskInfo.end());
						WaitingTaskInfo &w = it->second;
						assert(w._numReleaseAccesses > 0);
						w._numReleaseAccesses--;
						if (w._numReleaseAccesses == 0) {
							if (w._waitingTaskFinished != nullptr) {
								_stealableMessages.push_back(w._waitingTaskFinished);
							}
							_waitingTaskInfo.erase(it);
						}
					}
					break;

				case RELEASE_ACCESS_AND_FINISH:
				case TASK_FINISHED:
					// Nothing to do
					break;

				case DATA_RAW:
				case TOTAL_MESSAGE_TYPES:
					// We should never see these messages
					assert(0);
			}
		}

	public:

		// Called by worker to steal a message
		static T *stealMessage()
		{
			return _singleton.stealMessageInternal(/* isMessageHandlerItself */ false);
		}

		// Called by worker to notify finished handling a message
		static void notifyDone(T *msg)
		{
			_singleton.notifyDoneInternal(msg, /* isMessageHandlerItself */ false);
		}

		// When the function returns false the service stops.
		static bool executeService()
		{
			// The service is already unregistered, so finish it.
			if (!_singleton._live.load()) {
				return false;
			}

			if (_singleton._numWorkers == 0) {
				// No other worker tasks, so handle the messages right away and
				// sequentially here (more efficient than below)
				T *msg = nullptr;
				do {
					msg = ClusterManager::checkMail();

					if (msg != nullptr) {
						Instrument::clusterHandleMessage(msg, msg->getSenderId());
						const bool shouldDelete = msg->handleMessage();
						Instrument::clusterHandleMessage(msg, msg->getSenderId());

						if (shouldDelete) {
							delete msg;
						}
					}
				} while (msg != nullptr);

			} else {
				// At least one other worker, so let other threads steal messages
				bool keepGoing = true;
				while (keepGoing) {
					// First take all the messages (from the Messenger)
					T *msg = nullptr;
					do {
						msg = ClusterManager::checkMail();

						if (msg != nullptr) {
							// Queue the message, taking account of ordering constraints
							_singleton.queueMessage(msg);
						}
					} while (msg != nullptr);

					// Now handle a message
					msg = _singleton.stealMessageInternal(true);
					if (!msg) {
						keepGoing = false;
						break;
					}
					const bool shouldDelete = msg->handleMessage();
					_singleton.notifyDoneInternal(msg, true);

					if (shouldDelete) {
						delete msg;
					}
				}
			}

			return true;
		}

		static void registerService()
		{
			assert(_singleton._live.load() == false);
			_singleton._numWorkers = ClusterManager::getNumMessageHandlerWorkers();
			_singleton._numStolen = 0;
			_singleton._live = true;
		}

		static void unregisterService()
		{
			assert(_singleton._numStolen == 0);
			assert(_singleton._live.load() == true);
			_singleton._live = false;
		}
	};

	template <typename T> MessageHandler<T> MessageHandler<T>::_singleton;
}

#endif /* MESSAGE_HANDLER_HPP */
