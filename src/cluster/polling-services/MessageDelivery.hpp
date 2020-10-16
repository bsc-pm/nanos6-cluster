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
#include "system/PollingAPI.hpp"
#include "system/ompss/SpawnFunction.hpp"

#include "tasks/Task.hpp"
#include <Message.hpp>
#include <DataTransfer.hpp>
#include <ClusterManager.hpp>

#define TIMEOUT 500

class Message;
class DataTransfer;

namespace ClusterPollingServices {

	template<typename T>
	struct PendingQueue {
		PaddedSpinLock<> _lock;
		std::vector<T *> _pendings;
		std::atomic<bool> _live;

		static PendingQueue<T> _singleton;

		static void addPending(T *dt)
		{
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._lock);
			_singleton._pendings.push_back(dt);
		}

		static int checkDelivery()
		{
			std::vector<T *> &pendings = _singleton._pendings;
			std::lock_guard<PaddedSpinLock<>> guard(_singleton._lock);

			if (pendings.size() == 0) {
				//! We will only unregister this service from the
				//! ClusterManager at shutdown
				return 0;
			}

			ClusterManager::testCompletion<T>(pendings);

			pendings.erase(
				std::remove_if(
					pendings.begin(), pendings.end(),
					[](T *msg) {
						assert(msg != nullptr);

						bool completed = msg->isCompleted();
						if (completed) {
							delete msg;
						}

						return completed;
					}
				),
				std::end(pendings)
			);

			//! We will only unregister this service from the
			//! ClusterManager at shutdown
			return 0;
		}

		static void bodyCheckDelivery(__attribute__((unused)) void *args)
		{
			while (_singleton._live) {
				checkDelivery();
				nanos6_wait_for(TIMEOUT);
			}
		}
	};

	template<typename T>
	void registerPoolingDelivery()
	{
		PendingQueue<T>::_singleton._live = true;

		SpawnFunction::spawnFunction(
			PendingQueue<T>::bodyCheckDelivery,
			nullptr,
			nullptr,
			nullptr,
			"Message_Delivery",
			false,
			Task::nanos6_task_runtime_flag_t::nanos6_polling_flag
		);
	}

	template<typename T>
	void unregisterPoolingDelivery()
	{
		PendingQueue<T>::_singleton._live = false;

#ifndef NDEBUG
		std::lock_guard<PaddedSpinLock<>> guard(PendingQueue<T>::_singleton._lock);
		assert(PendingQueue<T>::_singleton._pendings.empty());
#endif
	}


template <typename T> PendingQueue<T> PendingQueue<T>::_singleton;

}

#endif /* MESSAGE_DELIVERY_HPP */
