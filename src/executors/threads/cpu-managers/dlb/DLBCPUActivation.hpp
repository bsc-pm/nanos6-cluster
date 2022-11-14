/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef DLB_CPU_ACTIVATION_HPP
#define DLB_CPU_ACTIVATION_HPP

#include <atomic>
#include <cassert>
#include <ctime>
#include <dlb.h>

#include "system/TrackingPoints.hpp"
#include "lowlevel/RWSpinLock.hpp"
#include "executors/threads/CPU.hpp"
#include "executors/threads/CPUManager.hpp"
#include "executors/threads/ThreadManager.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "scheduling/Scheduler.hpp"

class DLBCPUActivation {

public:
	static const char *stateNames[];

private:

	//! Amount of nanoseconds to delay if a CPU wants to be enabled but is
	//! being used by another process
	static timespec _delayCPUEnabling;

	//! The current number of OWNED active CPUs
	static std::atomic<size_t> _numActiveOwnedCPUs; // in enabled state
	static std::atomic<size_t> _numOwnedCPUs;
	static std::atomic<size_t> _numLentOwnedCPUs;   // in lent state
	static std::atomic<size_t> _numBorrowedCPUs;    // in acquired_enabled state
	static std::atomic<size_t> _numGivingCPUs;

private:

	static bool isActiveOwned(CPU::activation_status_t status)
	{
		return status == CPU::enabled_status;
	}

	static bool isOwned(CPU::activation_status_t status)
	{
		return status == CPU::enabled_status
		       || status == CPU::enabling_status
		       || status == CPU::lending_status
		       || status == CPU::lent_status
			   || status == CPU::uninitialized_given_status;
	}

	static bool isLentOwned(CPU::activation_status_t status)
	{
		return status == CPU::lent_status;
	}

	static bool isBorrowed(CPU::activation_status_t status)
	{
		return status == CPU::acquired_enabled_status;
	}

	static bool isGiving(CPU::activation_status_t status)
	{
		return status == CPU::giving_status;
	}

public:
	static inline bool changeStatusAndUpdateMetrics(CPU *cpu,
					CPU::activation_status_t oldStatus,
					CPU::activation_status_t newStatus)
	{
		bool successful = cpu->getActivationStatus().compare_exchange_strong(oldStatus, newStatus);
		if (successful) {
			int deltaActiveOwnedCPUs = isActiveOwned(newStatus) - isActiveOwned(oldStatus);
			int deltaOwnedCPUs       = isOwned(newStatus) - isOwned(oldStatus);
			int deltaLentOwnedCPUs   = isLentOwned(newStatus) - isLentOwned(oldStatus);
			int deltaBorrowedCPUs    = isBorrowed(newStatus) - isBorrowed(oldStatus);
			int deltaGivingCPUs      = isGiving(newStatus) - isGiving(oldStatus);
			_numActiveOwnedCPUs += deltaActiveOwnedCPUs;
			_numOwnedCPUs       += deltaOwnedCPUs;
			_numLentOwnedCPUs   += deltaLentOwnedCPUs;
			_numBorrowedCPUs    += deltaBorrowedCPUs;
			_numGivingCPUs      += deltaGivingCPUs;
			if (deltaOwnedCPUs + deltaGivingCPUs != 0) {
				Instrument::emitClusterEvent(Instrument::ClusterEventType::OwnedCPUs, _numOwnedCPUs + _numGivingCPUs);
			}
			if (deltaLentOwnedCPUs != 0) {
				Instrument::emitClusterEvent(Instrument::ClusterEventType::LentCPUs, _numLentOwnedCPUs);
			}
			if (deltaBorrowedCPUs != 0) {
				Instrument::emitClusterEvent(Instrument::ClusterEventType::BorrowedCPUs, _numBorrowedCPUs);
			}
			if (deltaGivingCPUs != 0) {
				Instrument::emitClusterEvent(Instrument::ClusterEventType::GivingCPUs, _numGivingCPUs);
			}
		}
		return successful;
	}

private:

	//! \brief Lend a CPU to another runtime or process
	//!
	//! \param[in] systemCPUId The id of the CPU to be lent
	//!
	//! \return The DLB error code returned by the call
	static inline int dlbLendCPU(size_t systemCPUId)
	{
		int ret = DLB_LendCpu(systemCPUId);
		if (ret != DLB_SUCCESS) {
			FatalErrorHandler::fail("DLB Error ", ret, " when trying to lend a CPU(", systemCPUId, ")");
		}

		return ret;
	}

	//! \brief Reclaim a previously lent CPU
	//!
	//! \param[in] systemCPUId The id of the CPU to be reclaimed
	//! \param[in] ignoreError Whether to disregard an error in the operation
	//!
	//! \return The DLB error code returned by the call
	static inline int dlbReclaimCPU(size_t systemCPUId, bool ignoreError = false)
	{
		int ret = DLB_ReclaimCpu(systemCPUId);
		if (!ignoreError) {
			if (ret != DLB_NOUPDT && ret != DLB_NOTED && ret != DLB_SUCCESS && ret != DLB_ERR_PERM) {
				// DLB_NOUPDT: not successful?
				// DLB_NOTED: should happen?
				// DLB_SUCCESS: successful
				// DLB_ERR_PERM: changed ownership (which is possible)
				FatalErrorHandler::fail("DLB Error ", ret, " when trying to reclaim a CPU(", systemCPUId, ")");
			}
		}

		return ret;
	}

	//! \brief Reclaim previously lent CPUs
	//!
	//! \param[in] numCPUs The number of CPUs to reclaim
	//!
	//! \return The DLB error code returned by the call
	static inline int dlbReclaimCPUs(size_t numCPUs)
	{
		// Try to take CPUs from the giving state
		for (size_t id = 0; id < (size_t)CPUManager::getTotalCPUs(); ++id) {
			CPU *cpu = CPUManager::getCPU(id);
			if (cpu->getActivationStatus() == CPU::giving_status) {
				bool successfulStatus = changeStatusAndUpdateMetrics(cpu, CPU::giving_status, CPU::lent_status);
				if (successfulStatus) {
					dlbEnableCallback(id, nullptr);
					numCPUs--;
					if (numCPUs ==0) {
						return DLB_SUCCESS;
					}
				}
			}
		}

		int ret = DLB_ReclaimCpus(numCPUs);
		if (ret != DLB_NOUPDT && ret != DLB_NOTED && ret != DLB_SUCCESS) {
			FatalErrorHandler::fail("DLB Error ", ret, " when trying to reclaim ", numCPUs, " CPUs");
		}

		return ret;
	}

	//! \brief Reclaim previously lent CPUs
	//!
	//! \param[in] maskCPUs A mask signaling which CPUs to reclaim
	//!
	//! \return The DLB error code returned by the call
	static inline int dlbReclaimCPUs(const cpu_set_t &maskCPUs)
	{
		int ret = DLB_ReclaimCpuMask(&(maskCPUs));
		if (ret != DLB_NOUPDT && ret != DLB_NOTED && ret != DLB_SUCCESS) {
			FatalErrorHandler::fail("DLB Error ", ret, " when reclaiming CPUs through a mask");
		}

		return ret;
	}

	//! \brief Try to acquire a specific number of CPUs
	//!
	//! \param[in] numCPUs The number of CPUs to acquire
	//!
	//! \return The DLB error code returned by the call
	static inline int dlbAcquireCPUs(size_t numCPUs)
	{
		int ret = DLB_AcquireCpus(numCPUs);
		if (ret != DLB_NOUPDT && ret != DLB_NOTED && ret != DLB_SUCCESS) {
			FatalErrorHandler::fail("DLB Error ", ret, " when trying to acquire ", numCPUs, " CPUs");
		}

		return ret;
	}

	//! \brief Try to acquire a specific number of CPUs
	//!
	//! \param[in] maskCPUs A mask signaling which CPUs to acquire
	//!
	//! \return The DLB error code returned by the call
	static inline int dlbAcquireCPUs(const cpu_set_t &maskCPUs)
	{
		// We may get DLB_ERR_PERM if we're trying to acquire unregistered CPUs
		int ret = DLB_AcquireCpuMask(&maskCPUs);
		if (ret != DLB_NOUPDT  &&
			ret != DLB_NOTED   &&
			ret != DLB_SUCCESS &&
			ret != DLB_ERR_PERM
		) {
			FatalErrorHandler::fail("DLB Error ", ret, " when trying to acquire CPUs through a mask");
		}

		return ret;
	}

	//! \brief Try to return a CPU
	//!
	//! \param[in] systemCPUId The id of the CPU to be returned
	//!
	//! \return The DLB error code returned by the call
	static inline int dlbReturnCPU(size_t systemCPUId)
	{
		// I think this will normally call the callback if it has to be returned
		int ret = DLB_ReturnCpu(systemCPUId);

		if (ret == DLB_ERR_PERM) {
			// Get DLB_ERR_PERM if a CPU we were using has been "stolen" using the DROM API
			// But the callback doesn't get called: call it manually
			dlbDisableCallback(systemCPUId, nullptr);

			ret = DLB_SUCCESS;
		}
		if (ret != DLB_SUCCESS && ret != DLB_NOUPDT) {
				FatalErrorHandler::fail("DLB Error ", ret, " when trying to return a CPU(", systemCPUId, ")");
		}

		return ret;
	}


public:

	//! \brief Check if a CPU is accepting new work
	static inline bool acceptsWork(CPU *cpu)
	{
		assert(cpu != nullptr);

		CPU::activation_status_t currentStatus = cpu->getActivationStatus();
		switch (currentStatus) {
			case CPU::enabled_status:
			case CPU::enabling_status:
			case CPU::acquired_status:
			case CPU::acquired_enabled_status:
				return true;
			case CPU::uninitialized_status:
			case CPU::uninitialized_given_status:
			case CPU::lent_status:
			case CPU::lending_status:
			case CPU::giving_status:
			case CPU::returned_status:
			case CPU::shutdown_status:
			case CPU::shutting_down_status:
				return false;
			case CPU::disabled_status:
			case CPU::disabling_status:
				assert(false);
				return false;
		}

		assert("Unhandled CPU activation status" == nullptr);
		return false;
	}

	//! \brief Enable a CPU
	//! NOTE: The enable/disable API has no effect if DLB is enabled
	//! (i.e., it has no effect in this implementation)
	//!
	//! \return False in this implementation
	static inline bool enable(size_t)
	{
		FatalErrorHandler::warn(
			"The enable/disable API is deactivated when using DLB. ",
			"No action will be performed"
		);

		return false;
	}

	//! \brief Disable a CPU
	//! NOTE: The enable/disable API has no effect if DLB is enabled
	//! (i.e., it has no effect in this implementation)
	//!
	//! \return False in this implementation
	static inline bool disable(size_t)
	{
		FatalErrorHandler::warn(
			"The enable/disable API is deactivated when using DLB. ",
			"No action will be performed"
		);

		return false;
	}

	static inline void giveCPU(CPU *cpu)
	{
		// Only give away cores if DROM is enabled
		if (!DLBCPUManager::getDromEnabled()) {
			return;
		}

		assert(cpu != nullptr);
		if (!Scheduler::isServingTasks()) {
			// No CPU is serving tasks in the scheduler, so don't give away a CPU yet, not sure why (?)
			return;
		}

		// Atomically decrease number of active owned CPUs and make sure never less than 1
		// NOTE: will stay owned until actually given away
		size_t currentActiveCPUs = _numActiveOwnedCPUs.load();
		if (currentActiveCPUs <= 1) {
			// Need to keep at least one active CPU
			return;
		}
		bool successful =_numActiveOwnedCPUs.compare_exchange_strong(currentActiveCPUs, currentActiveCPUs - 1);
		if (!successful) {
			// Race condition on decreasing number of active CPUs, so give up
			return;
		}

		// Atomically change from enabled_status to giving_status
		changeStatusAndUpdateMetrics(cpu, CPU::enabled_status, CPU::giving_status);
		_numActiveOwnedCPUs++; // in either case we already decreased the number of active owned CPUs (to reserve one)

		// This thread becomes idle and the CPU is ready to be given away via DROM.
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		assert(currentThread != nullptr);
		TrackingPoints::cpuBecomesIdle(cpu, currentThread);
		ThreadManager::addIdler(currentThread);
		currentThread->switchTo(nullptr);
	}

	//! \brief Lend a CPU to another runtime
	//!
	//! \param[in] cpu The CPU to lend
	static inline void lendCPU(CPU *cpu)
	{
		assert(cpu != nullptr);

		// Only lend CPUs if LeWI is enabled
		if (!DLBCPUManager::getLewiEnabled()) {
			return;
		}

		size_t currentActiveCPUs = _numActiveOwnedCPUs.load();
		bool successful = false;

		// Always avoid lending the last owned CPU
		if (currentActiveCPUs > 1) { //while (!successful && currentActiveCPUs > 1) { // PMC infinite loop possible?
			successful = _numActiveOwnedCPUs.compare_exchange_strong(currentActiveCPUs, currentActiveCPUs - 1);
			if (successful) {
				// NOTE: The CPU should only be lent when no work is available
				// Thus, we take for granted that the status of the CPU is
				// 'enabled_status' (unless it is shutting down)
				bool successfulStatus = changeStatusAndUpdateMetrics(cpu, CPU::enabled_status, CPU::lending_status);
				_numActiveOwnedCPUs++; // Compensate for subtracting to reserve one above

				if (successfulStatus) {
					// NOTE: Lend the CPU and check if work was added while
					// lending it. This is to avoid a race condition between
					// adding tasks and idling CPUs. If a task is added after
					// we lend the CPU, try to acquire CPUs through DLB, and
					// the CPU we've just lent should be re-acquired
					dlbLendCPU(cpu->getSystemCPUId());

					if (!Scheduler::isServingTasks()) {
						// No CPU is serving tasks in the scheduler, change to enabling.
						// At some later time, checkCPUStatusTransitions.
						// will set it back to enabled and increase the number of active
						// owned CPUs back to where it was.
						successfulStatus = changeStatusAndUpdateMetrics(cpu, CPU::lending_status, CPU::enabling_status);
						assert(successfulStatus);

						dlbReclaimCPU(cpu->getSystemCPUId());
					} else {
						WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
						assert(currentThread != nullptr);

						// Runtime Tracking Point - A cpu becomes idle
						// The status change is only notified once we know that the CPU
						// has been lent sucessfully. Additionally, the instrumentation
						// must be called prior the status change to prevent overlapping
						// instrumentation calls with threads resuming on the lending CPU
						TrackingPoints::cpuBecomesIdle(cpu, currentThread);

						// Change the status since the lending was successful
						successfulStatus = changeStatusAndUpdateMetrics(cpu, CPU::lending_status, CPU::lent_status);
						assert(successfulStatus);

						// This thread becomes idle
						ThreadManager::addIdler(currentThread);
						currentThread->switchTo(nullptr);
					}

					// NOTE: When a thread is resumed in the CPU we've just lent, the
					// CPU may not be available yet. We check this through DLB in
					// 'checkCPUStatusTransitions'
				} else {
					// The status change should always work, as noted previously
					assert(cpu->getActivationStatus() == CPU::shutdown_status);
					return;
				}
			}
		}
	}

	//! \brief Try to reclaim a CPU
	//!
	//! \param[in] systemCPUId The CPU's id
	//! Returns true if we now have it (either reclaimed or already had it)
	static inline bool reclaimCPU(size_t systemCPUId)
	{
		// Only lend CPUs if LeWI is enabled
		if (!DLBCPUManager::getLewiEnabled()) {
			return false;
		}

		CPU *cpu = CPUManager::getCPU(systemCPUId);
		assert(cpu != nullptr);

		bool successful = false;
		int ret;
		while (!successful) {
			CPU::activation_status_t currentStatus = cpu->getActivationStatus();
			switch (currentStatus) {
				case CPU::lent_status:
					// Try to reclaim a CPU disregarding an error (?). We disregard any error
					// since the CPU may already have been reclaimed.
					ret = dlbReclaimCPU(systemCPUId, true);
					if (ret == DLB_SUCCESS) {
						// it is typically enabling_status or enabled_status
						// std::cout << "dlbReclaimCPU was a success: new state " << stateNames[cpu->getActivationStatus()] << "\n";
					}
					return ret == DLB_SUCCESS;

				case CPU::returned_status:
					// We lost ownership concurrently so cannot reclaim it any more
					return false;

				case CPU::disabled_status:
				case CPU::disabling_status:
				case CPU::uninitialized_status:
				case CPU::uninitialized_given_status:
				case CPU::acquired_status:
				case CPU::acquired_enabled_status:
				case CPU::giving_status:
					assert(false);
					return false;

				case CPU::lending_status:
					// In this case we iterate until the lending is complete
					break;

				case CPU::enabled_status:
				case CPU::enabling_status:
				case CPU::shutdown_status:
				case CPU::shutting_down_status:
					// The CPU should already be ours, abort the reclaim
					return true;
			}
		}
		return false;
	}

	static inline int reclaimCPUs(int numRequested)
	{
		int ncpus = CPUManager::getTotalCPUs();
		int numReclaimed = 0;
		for (int k=0; k<ncpus; k++) {
			CPU *cpu = CPUManager::getCPU(k);
			assert(cpu != nullptr);
			if (cpu->getActivationStatus() == CPU::lent_status) {
				// Owned and lent out
				bool successful = reclaimCPU(k);
				if (successful) {
					numReclaimed++;
					if (numReclaimed == numRequested) {
						break;
					}
				}
			}
		}
		return numReclaimed;
	}

	//! \brief Try to acquire a specific number of external CPUs
	//!
	//! \param[in] numCPUs The number of CPUs to acquire
	static inline void acquireCPUs(size_t numCPUs)
	{
		assert(numCPUs > 0);

		// Only try to acquire CPUs if LeWI is enabled
		if (!DLBCPUManager::getLewiEnabled()) {
			return;
		}

		dlbAcquireCPUs(numCPUs);
	}

	//! \brief Try to acquire a specific number of external CPUs
	//!
	//! \param[in] maskCPUS The mask of CPUs to acquire
	static inline void acquireCPUs(const cpu_set_t &maskCPUs)
	{
		assert(CPU_COUNT(&maskCPUs) > 0);

		// Only try to acquire CPUs if LeWI is enabled
		if (!DLBCPUManager::getLewiEnabled()) {
			return;
		}

		dlbAcquireCPUs(maskCPUs);
	}

	//! \brief Return an external CPU
	//!
	//! \param[in,out] cpu The CPU to return
	static inline void returnCPU(CPU *cpu)
	{
		assert(cpu != nullptr);

		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		assert(currentThread != nullptr);

		// Change the status to returned and return it
		bool successful = changeStatusAndUpdateMetrics(cpu, CPU::acquired_enabled_status, CPU::returned_status);

		if (successful) {
			// Runtime Tracking Point - A cpu becomes idle
			TrackingPoints::cpuBecomesIdle(cpu, currentThread);

			// Since we do not want to keep the CPU, we use lend instead of return
			__attribute__((unused)) int ret = dlbLendCPU(cpu->getSystemCPUId());
			assert(ret == DLB_SUCCESS);

			ThreadManager::addIdler(currentThread);
			currentThread->switchTo(nullptr);
		} else {
			std::cout << "returnCPU: unexpected status " << cpu->getActivationStatus() << "\n";
			assert(cpu->getActivationStatus() == CPU::shutdown_status);
		}
	}

	//! \brief DLB-specific callback called when a CPU is returned to us
	//!
	//! \param[in] systemCPUId The id of the CPU that can be used once again
	//! \param[in] args (Unused) DLB-compliant callback arguments
	static inline void dlbEnableCallback(int systemCPUId, void *)
	{
		CPU *cpu = CPUManager::getCPU(systemCPUId);
		assert(cpu != nullptr);

		bool successful = false;
		while (!successful) {
			CPU::activation_status_t currentStatus = cpu->getActivationStatus();
			switch (currentStatus) {
				case CPU::disabled_status:  // It was given away, now it has been given back
				case CPU::disabling_status:
				case CPU::giving_status:
					// The CPU should never be in here in this implementation
					std::cout << "unexpected state in dlbEnableCallback " << currentStatus << "\n";
					assert(false);
					return;
				case CPU::enabled_status:
				case CPU::enabling_status:
				case CPU::acquired_status:
				case CPU::acquired_enabled_status:
				case CPU::shutdown_status:
					// If we are not expecting a callback, ignore it
					successful = true;
					break;
				case CPU::lending_status:
					// In this case we iterate until the lending is complete
					break;
				case CPU::uninitialized_status:
				case CPU::uninitialized_given_status:

					// If the status is uninit, this CPU must be external
					// We have acquired, for the first time, a CPU we don't own
					if (currentStatus == CPU::uninitialized_given_status) {
						successful = changeStatusAndUpdateMetrics(cpu, currentStatus, CPU::enabling_status);
					} else {
						successful = changeStatusAndUpdateMetrics(cpu, currentStatus, CPU::acquired_status);
					}
					if (successful) {
						// Initialize the acquired CPU
						cpu->initialize();

						// Resume a thread so it sees the status change
						ThreadManager::resumeIdle(cpu, true);
					}
					break;

				case CPU::lent_status:

					// If we are expecting a callback, try to reenable the CPU
					successful = changeStatusAndUpdateMetrics(cpu, CPU::lent_status, CPU::enabling_status);
					if (successful) {
						// Resume a thread so it sees the status changes
						ThreadManager::resumeIdle(cpu);
					}
					break;
				case CPU::returned_status:

					successful = changeStatusAndUpdateMetrics(cpu, CPU::returned_status, CPU::acquired_status);
					if (successful) {
						// Resume a thread so it sees the status change
						ThreadManager::resumeIdle(cpu);
					}
					break;
				case CPU::shutting_down_status:

					// If the CPU was lent and the runtime is shutting down
					// reenable the CPU without changing the status
					successful = changeStatusAndUpdateMetrics(cpu, currentStatus, CPU::shutdown_status);
					if (successful) {
						// Runtime Tracking Point - A cpu becomes active
						TrackingPoints::cpuBecomesActive(cpu);

						ThreadManager::resumeIdle(cpu, true, true);
					}
					break;
			}
		}
	}

	//! \brief DLB-specific callback called when a CPU is asked to be disabled
	//!
	//! \param[in] systemCPUId The id of the CPU to disable
	//! \param[in] args (Unused) DLB-compliant callback arguments
	static inline void dlbDisableCallback(int systemCPUId, void *)
	{
		CPU *cpu = CPUManager::getCPU(systemCPUId);
		WorkerThread *currentThread;
		assert(cpu != nullptr);

		bool successful = false;
		while (!successful) {
			CPU::activation_status_t currentStatus = cpu->getActivationStatus();
			switch (currentStatus) {
				// The CPU should never be in here in this implementation
				case CPU::uninitialized_status:
				case CPU::uninitialized_given_status:
				case CPU::disabled_status:
				case CPU::disabling_status:
				case CPU::giving_status:
					assert(false);
					return;
				// The CPU is unowned, cannot be in here
				case CPU::enabled_status:
				case CPU::enabling_status:
				case CPU::lent_status:
				case CPU::lending_status:
				// The CPU should be acquired, cannot be in here
				case CPU::shutting_down_status:
					assert(false);
					return;

				case CPU::returned_status:
					std::cout << "got dlbDisableCallback when already returned (I think this can happen due to a race condition and is harmless)\n";
					return;

				case CPU::acquired_status:
				case CPU::acquired_enabled_status:
					// If the callback is called at this point, it means the
					// CPU is checking if it should be returned, thus we can
					// assure no task is being executed and we can return it

					// We always check at each iteration of threads (see WorkerThread.cpp) if the CPU
					// they are running on must be returned. If they are to be returned, this callback
					// is called, and before changing the status of the CPU we call TrackingPoints to
					// avoid race conditions
					currentThread = WorkerThread::getCurrentWorkerThread();
					assert(currentThread != nullptr);

					// Runtime Tracking Point - A cpu becomes idle
					TrackingPoints::cpuBecomesIdle(cpu, currentThread);

					successful = changeStatusAndUpdateMetrics(cpu, currentStatus, CPU::returned_status);
					assert(successful);
					assert(currentThread->getTask() == nullptr);
					assert(currentThread->getComputePlace() == cpu);

					// The thread becomes idle
					ThreadManager::addIdler(currentThread);
					currentThread->switchTo(nullptr);
					break;

				case CPU::shutdown_status:
					// If the callback is called at this point, the runtime is
					// shutting down. Instead of returning the CPU, simply use
					// it to shutdown faster and DLB_Finalize will take care of
					// returning the CPU afterwards
					successful = true;
					break;
			}
		}
	}

	//! \brief Check and handle CPU activation transitions
	//! NOTE: This code must be run regularly from within WorkerThreads
	//!
	//! \param[in] currentThread The WorkerThread is currently checking
	//! transitions of the CPU it is running on
	//!
	//! \return The current status of the CPU the thread is running on
	static inline CPU::activation_status_t checkCPUStatusTransitions(WorkerThread *currentThread)
	{
		assert(currentThread != nullptr);

		CPU::activation_status_t currentStatus;
		bool successful = false;
		int ret;
		while (!successful) {
			CPU *cpu = currentThread->getComputePlace();
			assert(cpu != nullptr);

			currentStatus = cpu->getActivationStatus();
			switch (currentStatus) {
				case CPU::uninitialized_status:
				case CPU::uninitialized_given_status:
				case CPU::disabled_status:
				case CPU::disabling_status:
				case CPU::lending_status:
				case CPU::giving_status:
					// The CPU should never be disabled in this implementation
					// nor see itself as lending
					assert(false);
					return currentStatus;
				case CPU::enabled_status:
				case CPU::acquired_enabled_status:
				case CPU::shutdown_status:
					// If the CPU is enabled or shutdown, do nothing
					successful = true;
					break;
				case CPU::lent_status:
				case CPU::returned_status:
				case CPU::shutting_down_status:
					std::cout << "Thread awake in unexpected state " << currentStatus << "\n";
					// If a thread is woken up in this CPU but it is not supposed to be running, re-add
					// the thread as idle. We don't call TrackingPoints here since this is an extreme
					// case that should barely happen and the thread directly becomes idle again
					ThreadManager::addIdler(currentThread);
					currentThread->switchTo(nullptr);
					break;
				case CPU::enabling_status:
					// Might be returned to us but no longer owned because of DROM

					// If the CPU is owned and enabled, DLB may have woken us
					// even though another process may be using our CPU still
					// Before completing the enable, check if this is the case
					ret = DLB_CheckCpuAvailability(cpu->getSystemCPUId());
					if (ret != DLB_SUCCESS) {
						// The CPU is not ready yet, sleep for a bit
						nanosleep(&_delayCPUEnabling, nullptr);
					} else {
						successful = changeStatusAndUpdateMetrics(cpu, currentStatus, CPU::enabled_status);
						if (successful) {
							// This owned CPU becomes active now
							// ++_numActiveOwnedCPUs;
							currentStatus = CPU::enabled_status;

							// Runtime Tracking Point - A cpu becomes active
							TrackingPoints::cpuBecomesActive(cpu);
						}
					}
					break;
				case CPU::acquired_status:
					successful = changeStatusAndUpdateMetrics(cpu, currentStatus, CPU::acquired_enabled_status);
					if (successful) {
						currentStatus = CPU::acquired_enabled_status;

						// Runtime Tracking Point - A cpu becomes active
						TrackingPoints::cpuBecomesActive(cpu);
					}
					break;
			}
		}

		// Return the current status, whether there was a change
		return currentStatus;
	}

	//! \brief Check whether the CPU has to be returned
	//! NOTE: This function must be run after executing a task and
	//! before checking the status transitions of the CPU
	//!
	//! \param[in] thread The WorkerThread is currently checking
	//! whether has to return the CPU it is running on
	static inline void checkIfMustReturnCPU(WorkerThread *thread)
	{
		CPU *cpu = thread->getComputePlace();
		assert(cpu != nullptr);

		// If the CPU is not owned check if it must be returned
		CPU::activation_status_t currentStatus = cpu->getActivationStatus();
		if (currentStatus == CPU::acquired_enabled_status) {
			// At the start of the checkIfMustReturnCPU call, the CPU might
			// already be returned, so we switch here to idle and switch
			// back quickly to active if the CPU was not returned
			TrackingPoints::cpuBecomesIdle(cpu, thread);

			int ret = dlbReturnCPU(cpu->getSystemCPUId());
			if (ret != DLB_SUCCESS) {
				// The CPU wasn't returned, so resume instrumentation. In case
				// it was returned, the appropriate thread will resume it
				TrackingPoints::cpuBecomesActive(cpu);
			}

		}
	}

	//! \brief Notify to a CPU that the runtime is shutting down
	//!
	//! \param[in] cpu The CPU that transitions to a shutdown status
	static inline void shutdownCPU(CPU *cpu)
	{
		assert(cpu != nullptr);

		bool successful = false;
		while (!successful) {
			CPU::activation_status_t currentStatus = cpu->getActivationStatus();
			switch (currentStatus) {
				// The CPU should not already be shutting down
				case CPU::shutdown_status:
				case CPU::shutting_down_status:
					assert(false);
					return;

				case CPU::uninitialized_status:
				case CPU::uninitialized_given_status:
					// Be safe and set the uninitialized states to shutdown too

				case CPU::disabled_status: // Ownership has been given away
				case CPU::disabling_status: // Ownership has been given away
				case CPU::enabled_status:
				case CPU::enabling_status:
				case CPU::acquired_status:
				case CPU::acquired_enabled_status:
				case CPU::returned_status:
				case CPU::giving_status:
					// If the CPU is enabled, enabling or returned, changing to
					// shutdown is enough
					// If the CPU is acquired, switch to shutdown. Instead of
					// returning it, we use it a short amount of time to speed
					// up the shutdown process and DLB_Finalize will take care
					// of returning it
					successful = changeStatusAndUpdateMetrics(cpu, currentStatus, CPU::shutdown_status);
					break;
				case CPU::lent_status:
					// If the CPU is lent, transition to a shutdown status
					// The risk of a shutting down status is that if it changes ownership in the meantime
					// the CPU will never shut down
					successful = changeStatusAndUpdateMetrics(cpu, currentStatus, CPU::shutdown_status);
					//if (successful) {
				//		dlbReclaimCPU(cpu->getSystemCPUId());
				//	}
					break;
				case CPU::lending_status:
					// Iterate until the CPU is lent
					break;
			}
		}
	}

	static int getCurrentActiveOwnedCPUs()
	{
		return _numActiveOwnedCPUs.load();
	}

	static int getCurrentLentOwnedCPUs()
	{
		return _numLentOwnedCPUs;
	}

	static int getCurrentOwnedCPUs()
	{
		return _numOwnedCPUs.load();
	}

	static int getCurrentBorrowedCPUs()
	{
		return _numBorrowedCPUs.load();
	}

	static int getCurrentOwnedOrGivingCPUs()
	{
		return _numOwnedCPUs + _numGivingCPUs;
	}

	static void pollDROM(bool setMyMask);

	static void updateCPUOwnership();

	static void initialize()
	{
		// Count up the initial number of owned CPUs
		int ncpus = CPUManager::getTotalCPUs();
		for (int k=0; k<ncpus; k++) {
			CPU *cpu = CPUManager::getCPU(k);
			// Check initial status: DROM updates are not
			// active yet so don't worry about race conditions
			if (cpu->isOwned()) {
				_numOwnedCPUs ++; // initialize
			}
		}
	}

	static int getMyProcessMask(cpu_set_t &mask);

	static void checkCPUstates(const char *when, bool setMask);

	static const char *getDLBErrorName(int state);
};


#endif // DLB_CPU_ACTIVATION_HPP
