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
#include <MessageTaskNew.hpp>
#include <MessageNoEagerSend.hpp>
#include <NodeNamespace.hpp>

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

		// Messages that cannot be stolen (and that have lower priority, only tasknew for now)
		std::vector<MessageTaskNew*> _nonStealableTaskNew;

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
				{
					// * TASK_NEW is generally too trivial: as it only requires queuing the task to
					// be created and submitted by the namespace but many of those may arrive
					// sequentially and from time to time the first one may require to wakeup the
					// namespace task which may be a relatively expensive call. So we enqueue those
					// messages here to process them latter in order too enqueue all the stealable
					// ones for the workers first.
					MessageTaskNew* tasknew = dynamic_cast<MessageTaskNew *>(msg);
					assert(tasknew != nullptr);
					_nonStealableTaskNew.push_back(tasknew);
				}
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
					const OffloadedTaskIdManager::OffloadedTaskId taskId
						= (msg->getType() == NO_EAGER_SEND)
						? dynamic_cast<MessageNoEagerSend *>(msg)->getTaskId()
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

		// Finished handling a Message
		void notifyDoneInternal(T *msg, bool isMessageHandlerItself)
		{
			if (ClusterManager::getNumMessageHandlerWorkers() == 0) {
				return;
			};

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

		static void handleMessageWrapper(T *msg, bool isMessageHandlerItself)
		{
			Instrument::clusterHandleMessage(1, &msg, 1);
			const bool autodelete = msg->handleMessage();
			Instrument::clusterHandleMessage(1, &msg, 0);

			if (_singleton._numWorkers > 0) {
				_singleton.notifyDoneInternal(msg, isMessageHandlerItself);
			}

			if (autodelete) {
				delete msg;
			}
		}

		// Called by worker to steal a message
		static T *stealMessage(bool isMessageHandlerItself)
		{
			// Try to take a stealable task
			Message *msg = nullptr;

			std::lock_guard<PaddedSpinLock<>> guard(_singleton._lock);
			if (!_singleton._stealableMessages.empty()) {
				msg = _singleton._stealableMessages.front();
				_singleton._stealableMessages.pop_front();
				assert(msg != nullptr);

				if (!isMessageHandlerItself) {
					_singleton._numStolen.fetch_add(1);
				}
			}

			return msg;
		}

		// When the function returns false the service stops.
		static bool executeService()
		{
			// The service is already unregistered, so finish it.
			if (!_singleton._live.load()) {
				return false;
			}

			T *msg = nullptr;

			if (_singleton._numWorkers == 0) {
				// No other worker tasks, so handle the messages right away and sequentially here
				// (more efficient than below)
				while ((msg = ClusterManager::checkMail()) != nullptr) {
					handleMessageWrapper(msg, true);
				};

			} else {
				// At least one other worker, so let other threads steal messages
				while (true) {
					// First take all the messages (from the Messenger)
					while ((msg = ClusterManager::checkMail()) != nullptr) {
						// Queue the message, taking account of ordering constraints
						_singleton.queueMessage(msg);
					}

					// Now handle ONE message
					msg = stealMessage(true);

					// If not, then try to process non stealable messages
					if (msg == nullptr) {
						// Enqueue all the taskNew if possible taking the lock only once but also
						// calling tryWakeUp also only one time (tryWakeUp) may be expensive because
						// it calls the BlockingAPI.
						if (!_singleton._nonStealableTaskNew.empty()) {
							Instrument::clusterHandleMessage(
								_singleton._nonStealableTaskNew.size(),
								(Message **)_singleton._nonStealableTaskNew.data(), 1);

							NodeNamespace::enqueueMessagesTaskNew(_singleton._nonStealableTaskNew);

							Instrument::clusterHandleMessage(
								_singleton._nonStealableTaskNew.size(),
								(Message **)_singleton._nonStealableTaskNew.data(), 0);

							_singleton._nonStealableTaskNew.clear();
							// We added many tasks at once, so we can continue because this is
							// equivalent to handling many tasknews.
							continue;
						}

						// Set the action message in the namespace if
						if (_singleton._numStolen.load() == 0
							&& _singleton._nonStealableLowPriorityMessage != nullptr) {
							msg = _singleton._nonStealableLowPriorityMessage;
							_singleton._nonStealableLowPriorityMessage = nullptr;
						}
					}

					if (msg == nullptr) {
						break;
					}

					handleMessageWrapper(msg, true);
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
