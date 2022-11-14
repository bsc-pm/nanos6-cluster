/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "DLBCPUActivation.hpp"
#include "executors/threads/CPUManager.hpp"

#include "dlb.h"
#include "dlb_drom.h"


// 0.1 ms
timespec DLBCPUActivation::_delayCPUEnabling({0, 100000});
std::atomic<size_t> DLBCPUActivation::_numActiveOwnedCPUs(0);
std::atomic<size_t> DLBCPUActivation::_numOwnedCPUs(0);
std::atomic<size_t> DLBCPUActivation::_numLentOwnedCPUs(0);
std::atomic<size_t> DLBCPUActivation::_numBorrowedCPUs(0);
std::atomic<size_t> DLBCPUActivation::_numGivingCPUs(0);

// cpu_set_t DLBCPUActivation::_ownedByMe;
const char *DLBCPUActivation::stateNames[] =  {
		"uninitialized_status",
		"enabled_status",           // The CPU is enabled
		"enabling_status",          // The CPU is being enabled
		"disabled_status",          // The CPU is disabled
		"disabling_status",         // The CPU is being disabled
		"lent_status",              // The CPU is lent to another runtime
		"lending_status",           // The CPU is about to be lent to another runtime
		"giving_status",            // The CPU is about to be given away (via DROM)
		"uninitialized_given_status", // The CPU has been given to us via DROM but has not been initialized
		"acquired_status",          // The (external) CPU is acquired but not enabled yet
		"acquired_enabled_status",  // The (external) CPU is acquired and enabled
		"returned_status",          // The (external) CPU is returned to its original owner
		"shutting_down_status",     // The CPU is being shutdown
		"shutdown_status"           // The CPU is shutdown and shouldn't be used
	};

void DLBCPUActivation::pollDROM(bool setMask)
{
	int newCountCPUs;
	cpu_set_t newMask;
	int ret = DLB_PollDROM(&newCountCPUs, &newMask);
	if (setMask) {
		// Probably no longer important
		if (ret != DLB_SUCCESS) {
			std::cout << "DLB_PollDROM returned bad " << ret << "\n";
		}
		assert(ret == DLB_SUCCESS);
	}

	if (ret == DLB_SUCCESS) {
		// Take account of new mask and number of CPUs

		for (size_t id = 0; id < (size_t)CPUManager::getTotalCPUs(); ++id) {
			CPU *cpu = CPUManager::getCPU(id);

			if (CPU_ISSET(id, &newMask)) {
				bool done = false;
				__attribute__((unused)) bool successful;
				while (!done) {
					done = true;

					/* CPU is available through DROM */
					CPU::activation_status_t currentStatus = cpu->getActivationStatus();
					CPU::activation_status_t newStatus = currentStatus;
					bool incOwned = false;
					switch (currentStatus) {
						// All these imply that the CPU was already ours, so do nothing
						case CPU::shutdown_status:
						case CPU::enabled_status:
						case CPU::enabling_status:
						case CPU::giving_status:
							break;

						// Should never happen
						case CPU::disabled_status: // Ownership has been given away
						case CPU::disabling_status: // Ownership has been given away
						case CPU::uninitialized_given_status:
							assert(false);
							break;

						// Taking ownership of a core that we haven't seen before
						case CPU::uninitialized_status:
							{
								CPU::activation_status_t newStatus2 = CPU::uninitialized_given_status;
								successful = changeStatusAndUpdateMetrics(cpu, currentStatus, newStatus2);
								assert(successful);
								cpu->setOwned(true);
							}
							dlbEnableCallback(id, nullptr);
							currentStatus = cpu->getActivationStatus();
							newStatus = currentStatus;
							break;

						case CPU::returned_status:
							// Move first from returned to lent, then call enable callback
							newStatus = CPU::lent_status;
							incOwned = true;
							done = false;
							break;

						// Taking ownership of a core that we were borrowing
						case CPU::lending_status:
						case CPU::acquired_status:
							done = false;
							// std::cout << "acquired_status " << currentStatus << " in pollDROM: try again (wait to move to acquired_enabled_status)\n";
							break;

						case CPU::acquired_enabled_status:
							newStatus = CPU::enabled_status;
							break;

						case CPU::shutting_down_status:
							// Shutting down status is used when a CPU was lent when the shutdown
							// started. It may instead be returned via DROM instead.
						case CPU::lent_status:
							break;
						}
					// Change status
					if (newStatus != currentStatus) {
						successful = changeStatusAndUpdateMetrics(cpu, currentStatus, newStatus);
						assert(successful);
					}
					if (incOwned) {
						dlbEnableCallback(id, nullptr);

						// Set as owned for indicative purposes: note that there is a race condition between
						// the current number of owned CPUs, the activation status and whether it is set as owned.
						// So do not use the owned state where race conditions may be important.
						cpu->setOwned(true);
					}
				}
			} else if (!CPU_ISSET(id, &newMask)) {
				// Don't do anything yet, wait for a reclaim via
				// DLB_ReturnCPU

				/* CPU becomes UNavailable through DROM */

				// Clear owned flag for indicative purposes: note that there is a race condition between
				// the current number of owned CPUs, the activation status and whether it is set as owned.
				// So do not use the owned state where race conditions may be important.
				cpu->setOwned(false);

				// Don't do anything else: wait until the affected CPU(s) are reclaimed
				CPU::activation_status_t currentStatus = cpu->getActivationStatus();
				switch (currentStatus) {

					// We still don't have a CPU we never had
					case CPU::uninitialized_status:
						break;

					// We still don't own a CPU that we already didn't own
					case CPU::acquired_status:
					case CPU::acquired_enabled_status:
						break;

					case CPU::returned_status:
						// This is the expected state immediately after the CPU activation status was set,
						// because the activation status is set at the same time as the DROM process mask was updated.
						break;

					// All these imply that a CPU was stolen from us: should not happen in this implementation
					// unless there is another rogue process on the same node
					case CPU::uninitialized_given_status:
					case CPU::shutdown_status:
					case CPU::shutting_down_status:
					case CPU::enabled_status:
					case CPU::enabling_status:
					case CPU::giving_status:
					case CPU::disabled_status: // Ownership has been given away
					case CPU::disabling_status: // Ownership has been given away
					case CPU::lending_status:
					case CPU::lent_status:
						std::cout << "Unexpected status: " << currentStatus << "\n";
						assert(false);
						break;
				}
			}
		}
		// memcpy(&ownedByMe, &newMask, sizeof(cpu_set_t));
	} else if (ret == DLB_NOUPDT) {
		// No update: nothing to do
	} else if (ret == DLB_ERR_DISBLD) {
		assert(false); // should not be disabled
	} else {
		assert(false); // no other possible return values
	}
}

void DLBCPUActivation::updateCPUOwnership()
{
}

#define VALID_DLB_SUCCESS 1
#define VALID_DLB_NOTED 2
#define VALID_DLB_NOUPDT 4
#define VALID_DLB_ERR_PERM 8
#define VALID_DLB_BORROWED 16
#define VALID_DLB_ALL 31

void DLBCPUActivation::checkCPUstates(const char *when, bool setMask)
{
	(void)setMask;

	size_t numActiveOwnedCPUs = 0;
	size_t numOwnedCPUs = 0;
	size_t numLentOwnedCPUs = 0;
	size_t numBorrowedCPUs = 0;    // in acquired_enabled state
	size_t numGivingCPUs = 0;    // in acquired_enabled state

	for (size_t id = 0; id < (size_t)CPUManager::getTotalCPUs(); ++id) {
		CPU *cpu = CPUManager::getCPU(id);
		CPU::activation_status_t currentStatus = cpu->getActivationStatus();
		/*! \brief Check whether the specified CPU is being used by another process
		 *  \param[in] cpuid CPU to be checked
		 *  \return DLB_SUCCESS if the CPU is available
		 *  \return DLB_NOTED if the CPU is owned but still guested by other process
		 *  \return DLB_NOUPDT if the CPU is owned but still not reclaimed
		 *  \return DLB_ERR_PERM if the CPU cannot be acquired or has been disabled
		 *  \return DLB_ERR_DISBLD if DLB is disabled
		 */
		 switch (currentStatus) {
			case CPU::uninitialized_given_status:
			case CPU::uninitialized_status:
			case CPU::shutdown_status:
			case CPU::shutting_down_status:
				break;

			case CPU::enabled_status:
				numActiveOwnedCPUs ++;
				numOwnedCPUs ++;
				break;

			case CPU::enabling_status:
			case CPU::lending_status:
				numOwnedCPUs ++;
				break;

			case CPU::giving_status:
				numGivingCPUs ++;
				break;

			case CPU::lent_status:
				numOwnedCPUs ++;
				numLentOwnedCPUs ++;
				break;

			case CPU::disabled_status:
			case CPU::disabling_status:
				assert(false);
				break;

			case CPU::returned_status:
				break;

			case CPU::acquired_status:
				break;

			case CPU::acquired_enabled_status:
				numBorrowedCPUs++;
				break;
		 }
	}

	if ((numActiveOwnedCPUs != _numActiveOwnedCPUs)
	    || (numOwnedCPUs != _numOwnedCPUs)
		|| (numLentOwnedCPUs != _numLentOwnedCPUs)
		|| (numBorrowedCPUs != _numBorrowedCPUs)
		|| (numGivingCPUs != _numGivingCPUs)) {
		std::cout << when << "Miscount on extrank " << ClusterManager::getExternalRank()
		          << " active owned: actual " << numActiveOwnedCPUs << " vs " << _numActiveOwnedCPUs
		          << " owned: actual " << numOwnedCPUs << " vs " << _numOwnedCPUs
		          << " lent: actual " << numLentOwnedCPUs << " vs " << _numLentOwnedCPUs
		          << " giving: actual " << numGivingCPUs << " vs " << _numGivingCPUs
		          << " borrowed: actual " << numBorrowedCPUs << " vs " << _numBorrowedCPUs << "\n";
	}
}

int DLBCPUActivation::getMyProcessMask(cpu_set_t &mask)
{
	int mypid = getpid();
	int ret = DLB_DROM_GetProcessMask(mypid, &mask, (dlb_drom_flags_t)0);
	if (ret != DLB_SUCCESS) {
		std::cout << "DLB_DROM_GetProcessMask: return value " << ret << "\n";
		assert(false);
	}

	for (size_t id = 0; id < (size_t)CPUManager::getTotalCPUs(); ++id) {
		CPU *cpu = CPUManager::getCPU(id);
		CPU::activation_status_t currentStatus = cpu->getActivationStatus();
		bool owned = CPU_ISSET(id, &mask);
		bool expectedOwned;
		switch (currentStatus) {

			case CPU::uninitialized_status:
			case CPU::returned_status:
			case CPU::acquired_status:
			case CPU::acquired_enabled_status:
				expectedOwned = false;
				break;

			case CPU::uninitialized_given_status:
			case CPU::shutdown_status:
			case CPU::shutting_down_status:
			case CPU::enabled_status:
			case CPU::enabling_status:
			case CPU::giving_status:
			case CPU::lending_status:
			case CPU::lent_status:

				expectedOwned = true;
				break;

			case CPU::disabled_status:
			case CPU::disabling_status:
				assert(false);
				break;
		 }
		 if (owned != expectedOwned) {
			std::cout << "Extrank " << ClusterManager::getExternalRank()
					<< ": state " << stateNames[currentStatus] << " but owned = " << owned << "\n";
		 }
	}
	return DLB_SUCCESS;
}

const char *DLBCPUActivation::getDLBErrorName(int state)
{
	switch(state) {
		case DLB_NOUPDT:		/*! The operation cannot be performed now, but it has been attended */
			return "DLB_NOUPDT";
		case DLB_NOTED:			/*! Success */
			return "DLB_NOTED";
		case DLB_SUCCESS:			/*! Unknown error */
			return "DLB_SUCCESS";
		case DLB_ERR_UNKNOWN:			/*! DLB has not been initialized */
			return "DLB_ERR_UNKNOWN";
		case DLB_ERR_NOINIT:			/*! DLB is already initialized */
			return "DLB_ERR_NOINIT";
		case DLB_ERR_INIT:			/*! DLB is disabled */
			return "DLB_ERR_INIT";
		case DLB_ERR_DISBLD:			/*! DLB cannot find a shared memory */
			return "DLB_ERR_DISBLD";
		case DLB_ERR_NOSHMEM:			/*! DLB cannot find the requested process */
			return "DLB_ERR_NOSHMEM";
		case DLB_ERR_NOPROC:			/*! DLB cannot update the target process, operation still in process */
			return "DLB_ERR_NOPROC";
		case DLB_ERR_PDIRTY:			/*! DLB cannot acquire the requested resource */
			return "DLB_ERR_PDIRTY";
		case DLB_ERR_PERM:			/*! The operation has timed out */
			return "DLB_ERR_PERM";
		case DLB_ERR_TIMEOUT:			/*! The callback is not defined and cannot be invoked */
			return "DLB_ERR_TIMEOUT";
		case DLB_ERR_NOCBK:			/*! The queried entry does not exist */
			return "DLB_ERR_NOCBK";
		case DLB_ERR_NOENT:			/*! The operation is not compatible with the configured DLB options  */
			return "DLB_ERR_NOENT";
		case DLB_ERR_NOCOMP:			/*! DLB cannot take more requests for a specific resource */
			return "DLB_ERR_NOCOMP";
		case DLB_ERR_REQST:			/*! DLB cannot allocate more processes into the shared memory */
			return "DLB_ERR_REQST";
		case DLB_ERR_NOMEM:			/*! The operation is not defined in the current polic */
			return "DLB_ERR_NOMEM";
		case DLB_ERR_NOPOL:
			return "DLB_ERR_NOPOL";
		case DLB_ERR_NOTALP:
			return "DLB_ERR_NOTALP";
		case 100:
			return "DLB_BORROWED";
		default:
			return "unknown DLB error";
	}
}
