/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include <cassert>

#include <nanos6.h>

#include <InstrumentThreadInstrumentationContext.hpp>
#include <InstrumentUserMutex.hpp>

#include "DataAccessRegistration.hpp"
#include "TaskBlocking.hpp"
#include "UserMutex.hpp"
#include "executors/threads/CPUManager.hpp"
#include "executors/threads/ThreadManager.hpp"
#include "executors/threads/ThreadManagerPolicy.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "lowlevel/SpinLock.hpp"
#include "scheduling/Scheduler.hpp"
#include "system/TrackingPoints.hpp"
#include "tasks/Task.hpp"
#include "tasks/TaskImplementation.hpp"
#include "cluster/hybrid/ClusterStats.hpp"


typedef std::atomic<UserMutex *> mutex_t;


void nanos6_user_lock(void **handlerPointer, __attribute__((unused)) char const *invocationSource)
{
	UserMutex *userMutex = nullptr;

	assert(handlerPointer != nullptr);
	mutex_t &userMutexReference = (mutex_t &) *handlerPointer;

	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	assert(currentThread != nullptr);

	Task *currentTask = currentThread->getTask();
	assert(currentTask != nullptr);

	// Runtime Tracking Point - Entering a user lock
	TrackingPoints::enterUserLock(currentTask);

	ComputePlace *computePlace = currentThread->getComputePlace();
	assert(computePlace != nullptr);

	// Allocation
	if (__builtin_expect(userMutexReference == nullptr, 0)) {
		UserMutex *newMutex = new UserMutex(true);

		UserMutex *expected = nullptr;
		if (userMutexReference.compare_exchange_strong(expected, newMutex)) {
			// Successfully assigned new mutex
			assert(userMutexReference == newMutex);

			// Since we allocate the mutex in the locked state, the thread already owns it and the work is done
			goto end;
		} else {
			// Another thread managed to initialize it before us
			assert(expected != nullptr);
			assert(userMutexReference == expected);

			delete newMutex;

			// Continue through the "normal" path
		}
	}

	// The mutex has already been allocated and cannot change, so skip the atomic part from now on
	userMutex = userMutexReference.load();

	if (currentTask->isTaskfor()) {
		// Lock the mutex directly
		userMutex->spinLock();
	} else {
		// Fast path
		if (userMutex->tryLock()) {
			goto end;
		}

		// Acquire the lock if possible. Otherwise queue the task.
		if (userMutex->lockOrQueue(currentTask)) {
			// Successful
			goto end;
		}

		Instrument::taskIsBlocked(currentTask->getInstrumentationTaskId(), Instrument::in_mutex_blocking_reason);

		ClusterStats::leaveTask(currentThread);
		Instrument::blockedOnUserMutex(userMutex);

		TaskBlocking::taskBlocks(currentThread, currentTask);

		// Update the CPU since the thread may have migrated
		computePlace = currentThread->getComputePlace();
		assert(computePlace != nullptr);
		Instrument::ThreadInstrumentationContext::updateComputePlace(computePlace->getInstrumentationId());

		// This in combination with a release from other threads makes their changes visible to this one
		std::atomic_thread_fence(std::memory_order_acquire);

		Instrument::taskIsExecuting(currentTask->getInstrumentationTaskId(), true);
		ClusterStats::returnToTask(currentThread);
	}

end:
	Instrument::acquiredUserMutex(userMutex);

	// Runtime Tracking Point - Exiting a user lock
	TrackingPoints::exitUserLock(currentTask);
}


void nanos6_user_unlock(void **handlerPointer)
{
	assert(handlerPointer != nullptr);
	assert(*handlerPointer != nullptr);

	// This in combination with an acquire from another thread makes the changes visible to that one
	std::atomic_thread_fence(std::memory_order_release);

	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	assert(currentThread != nullptr);

	Task *currentTask = currentThread->getTask();
	assert(currentTask != nullptr);

	// Runtime Tracking Point - Entering user unlock
	TrackingPoints::enterUserUnlock(currentTask);

	mutex_t &userMutexReference = (mutex_t &) *handlerPointer;
	UserMutex &userMutex = *(userMutexReference.load());
	Instrument::releasedUserMutex(&userMutex);

	Task *releasedTask = userMutex.dequeueOrUnlock();
	if (releasedTask != nullptr) {
		CPU *cpu = currentThread->getComputePlace();
		assert(cpu != nullptr);

		if (!currentTask->isTaskfor() && ThreadManagerPolicy::checkIfUnblockedMustPreemptUnblocker(currentTask, releasedTask, cpu)) {
			WorkerThread *releasedThread = releasedTask->getThread();
			assert(releasedThread != nullptr);

			// Try to get an unused CPU and offload the released task's execution in it
			CPU *obtainedCPU = (CPU *) CPUManager::getUnusedCPU();
			if (obtainedCPU != nullptr) {
				releasedThread->resume(obtainedCPU, false);
			} else {
				// No idle CPUs available, first re-add the current task to the scheduler
				Scheduler::addReadyTask(currentTask, cpu, UNBLOCKED_TASK_HINT);

				// Runtime Tracking Point - A thread is about to be suspended
				TrackingPoints::threadWillSuspend(currentThread, cpu);

				// Now switch to the released thread
				currentThread->switchTo(releasedThread);

				// Update the CPU since the thread may have migrated
				cpu = currentThread->getComputePlace();
				assert(cpu != nullptr);
				Instrument::ThreadInstrumentationContext::updateComputePlace(cpu->getInstrumentationId());
			}
		} else {
			Scheduler::addReadyTask(releasedTask, cpu, UNBLOCKED_TASK_HINT);
		}
	}

	// Runtime Tracking Point - Exiting user unlock
	TrackingPoints::exitUserUnlock(currentTask);
}

