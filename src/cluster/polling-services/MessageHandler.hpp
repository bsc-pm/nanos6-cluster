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
#include <MessageNoEagerSend.hpp>

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
		std::atomic<size_t> _numStolen;

		// Messages that can be stolen by other workers
		std::deque<T*> _stealableMessages;

		// Messages that cannot be stolen (and that have lower priority)
		std::deque<T*> _nonStealableMessages;

		// Only one _nonStealableLowPriorityMessage may exist at the time as they are malleability
		// or the finish message.
		T* _nonStealableLowPriorityMessage;

		// For a given task, keep track of number of RELEASE_ACCESS or NO_EAGER_SEND messages
		// left to handle plus the TASK_FINISHED/RELEASE_ACCESS_AND_FINISH
		// message, if already received.
		struct WaitingTaskInfo {
			int _numPredecessors;
			Message *_waitingTaskFinished;
		};

		PaddedSpinLock<> _waitingTaskInfoLock;
		std::unordered_map<OffloadedTaskIdManager::OffloadedTaskId, WaitingTaskInfo> _waitingTaskInfo;

		// Queue a received message
		void queueMessage(T *msg)
		{
			switch(msg->getType()) {
				case DMALLOC:
				case DFREE:
				case DATA_FETCH:
				case DATA_SEND: // Not sure whether worth offloading to workers as just an MPI call
				case SATISFIABILITY:
				{
					// These messages have no ordering constraints and are worth handling
					// by other workers. All involve dependency system operations, which may
					// potentially be a lot of work. So put the message into _stealableMessages
					std::lock_guard<PaddedSpinLock<>> guard(_lock);
					_stealableMessages.push_back(msg);
				}
				break;

				case TASK_NEW:
					// * TASK_NEW is generally too trivial: as it only requires queuing the task to
					// be created and submitted by the namespace but many of those may arrive
					// sequentially and from time to time the first one may require to wakeup the
					// namespace task which may be a relatively expensive call. So we enqueue those
					// messages here to process them latter in order too enqueue all the stealable
					// ones for the workers first.
					_nonStealableMessages.push_back(msg);
					break;

				case SYS_FINISH:
					// * These messages are the last ones to be processes... they must be the last
					// * of all to be handled ones
					assert(_nonStealableLowPriorityMessage == nullptr);
					_nonStealableLowPriorityMessage = msg;
					break;

				case NO_EAGER_SEND:
				case RELEASE_ACCESS:
				{
					// This message is worth handling by other workers, but we need to ensure it
					// is done before handling the task's TASK_FINISHED. Increment the number of
					// RELEASE_ACCESSES or NO_EAGER_SENDs for this task.
					OffloadedTaskIdManager::OffloadedTaskId taskId
						= (msg->getType() == NO_EAGER_SEND) ?
							dynamic_cast<MessageNoEagerSend *>(msg)->getTaskId()
							: dynamic_cast<MessageReleaseAccess *>(msg)->getTaskId();
					{
						std::lock_guard<PaddedSpinLock<>> guard(_waitingTaskInfoLock);
						auto it = _waitingTaskInfo.find(taskId);
						if (it == _waitingTaskInfo.end()) {
							_waitingTaskInfo[taskId] = WaitingTaskInfo({1, nullptr});
						} else {
							it->second._numPredecessors++;
						}
					}

					std::lock_guard<PaddedSpinLock<>> guard(_lock);
					_stealableMessages.push_back(msg);
				}
				break;

				case TASK_FINISHED:
				case RELEASE_ACCESS_AND_FINISH:
				{
					OffloadedTaskIdManager::OffloadedTaskId taskId
						= dynamic_cast<MessageReleaseAccess *>(msg)->getTaskId();

					std::lock_guard<PaddedSpinLock<>> guard(_waitingTaskInfoLock);
					auto it = _waitingTaskInfo.find(taskId);

					if (it == _waitingTaskInfo.end()) {
						// No predecessors, so it can be handled immediately
						std::lock_guard<PaddedSpinLock<>> guard2(_lock);
						_stealableMessages.push_back(msg);
					} else {
						// There are some predecessor messages left to be handled first
						assert(it->second._waitingTaskFinished == nullptr);
						it->second._waitingTaskFinished = msg;
					}
				}
				break;

				case DATA_RAW:
				case TOTAL_MESSAGE_TYPES:
					// We should never see these messages
					FatalErrorHandler::fail(
						"Message handler tried to queue a message type: ", msg->getName()
					);
			}
		}

		// Steal a message
		T *stealMessageInternal(bool isMessageHandlerItself)
		{
			// Try to take a stealable task
			Message *msg = nullptr;

			_lock.lock();
			if (!_stealableMessages.empty()) {
				msg = _stealableMessages.front();
				_stealableMessages.pop_front();
				assert(msg != nullptr);

				if (!isMessageHandlerItself) {
					_numStolen.fetch_add(1);
				}
			}
			_lock.unlock();

			// If not, and this is the polling service itself, take a non-stealable one
			if (isMessageHandlerItself && msg == nullptr) {
				if (!_nonStealableMessages.empty()) {
					msg = _nonStealableMessages.front();
					_nonStealableMessages.pop_front();
					assert(msg != nullptr);
				} else if (_numStolen.load() == 0 && _nonStealableLowPriorityMessage != nullptr) {
					msg = _nonStealableLowPriorityMessage;
					_nonStealableLowPriorityMessage = nullptr;
				}
			}
			if (msg) {
				Instrument::clusterHandleMessage(msg, msg->getSenderId());
			}
			return msg;
		}

		// Finished handling a Message
		void notifyDoneInternal(T *msg, bool isMessageHandlerItself)
		{
			Instrument::clusterHandleMessage(msg, -1);

			if (!isMessageHandlerItself) {
				_numStolen.fetch_sub(1);
			}

			switch (msg->getType()) {
				case SYS_FINISH:
				case DMALLOC:
				case DFREE:
				case DATA_FETCH:
				case DATA_SEND:
				case TASK_NEW:
				case SATISFIABILITY:
				case RELEASE_ACCESS_AND_FINISH:
				case TASK_FINISHED:
					// These messages have no ordering constraints, so do nothing
					break;

				case NO_EAGER_SEND:
				case RELEASE_ACCESS:
					// After handling RELEASE_ACCESS or NO_EAGER_SEND, decrement the number of
					// predecessor messages that must be done before finishing the task
				{
					OffloadedTaskIdManager::OffloadedTaskId taskId
						= (msg->getType() == NO_EAGER_SEND) ?
							dynamic_cast<MessageNoEagerSend *>(msg)->getTaskId()
							: dynamic_cast<MessageReleaseAccess *>(msg)->getTaskId();

					std::lock_guard<PaddedSpinLock<>> guard1(_waitingTaskInfoLock);

					auto it = _waitingTaskInfo.find(taskId);
					assert(it != _waitingTaskInfo.end());

					WaitingTaskInfo &w = it->second;
					assert(w._numPredecessors > 0);
					w._numPredecessors--;
					if (w._numPredecessors == 0) {
						if (w._waitingTaskFinished != nullptr) {
							std::lock_guard<PaddedSpinLock<>> guard2(_lock);
							_stealableMessages.push_back(w._waitingTaskFinished);
						}
						_waitingTaskInfo.erase(it);
					}
				}
				break;

				case DATA_RAW:
				case TOTAL_MESSAGE_TYPES:
					// We should never see these messages
					FatalErrorHandler::fail(
						"Message handler tried to notifyDone a message type: ", msg->getName()
					);
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
			_singleton._numStolen.store(0);
			_singleton._live.store(true);
		}

		static void waitUntilFinished()
		{
			assert(_singleton._live.load() == true);
			executeService();
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
