/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CPU_HPP
#define CPU_HPP

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <atomic>
#include <cassert>
#include <deque>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>

#include "CPUThreadingModelData.hpp"
#include "hardware/places/CPUPlace.hpp"
#include "hardware-counters/CPUHardwareCounters.hpp"
#include "lowlevel/SpinLock.hpp"

#include <InstrumentComputePlaceManagement.hpp>


class WorkerThread;

class CPU : public CPUPlace {

public:

#ifdef __KNC__
	// For some reason ICPC from composer_xe_2015.2.164 with KNC is unable to handle the activation_status_t as is correctly
	typedef unsigned int activation_status_t;
#define activation_status_t knc_workaround_activation_status_t
#endif

	typedef enum {
		uninitialized_status=0,
		enabled_status,           // The CPU is enabled
		enabling_status,          // The CPU is being enabled
		disabled_status,          // The CPU is disabled
		disabling_status,         // The CPU is being disabled
		lent_status,              // The CPU is lent to another runtime
		lending_status,           // The CPU is about to be lent to another runtime
		giving_status,            // The CPU is about to be given away (via DROM)
		uninitialized_given_status, // The CPU has been given to us via DROM but has not been initialized
		acquired_status,          // The (external) CPU is acquired but not enabled yet
		acquired_enabled_status,  // The (external) CPU is acquired and enabled
		returned_status,          // The (external) CPU is returned to its original owner
		shutting_down_status,     // The CPU is being shutdown
		shutdown_status           // The CPU is shutdown and shouldn't be used
	} activation_status_t;

#ifdef __KNC__
#undef activation_status_t
#endif

private:

	std::atomic<activation_status_t> _activationStatus;

	size_t _systemCPUId;
	size_t _NUMANodeId;
	size_t _groupId;

	//! The CPU mask so that we can later on migrate threads to this CPU
	cpu_set_t _cpuMask;

	//! The pthread attr that is used for all the threads of this CPU
	//!
	//! Making changes in this attribute is *NOT* thread-safe. We assume that only
	//! one thread is touching this at a time.
	pthread_attr_t _pthreadAttr;

	//! Per-CPU data that is specific to the threading model
	CPUThreadingModelData _threadingModelData;

	//! The hardware counter structures of this CPU
	CPUHardwareCounters _hardwareCounters;

public:

	//! \brief Constructor for regular CPUs
	//!
	//! \param[in] systemCPUId The system id of the CPU
	//! \param[in] virtualCPUId The virtual id or index of the CPU
	//! \param[in] NUMANodeId The NUMA node id of the CPU
	CPU(size_t systemCPUId, size_t virtualCPUId, size_t NUMANodeId);

	//! \brief Constructor for virtual CPUs
	//!
	//! This function should only be used to construct virtual CPUs,
	//! like the CPU assigned to the leader thread. The system id is
	//! set to a completely invalid number, while the NUMA node id
	//! is set to the first valid NUMA node
	//!
	//! \param[in] virtualCPUId The virtual id or index of the CPU
	inline CPU(size_t virtualCPUId) :
		CPUPlace(virtualCPUId, false),
		_activationStatus(uninitialized_status),
		_systemCPUId((size_t) -1),
		_NUMANodeId(0),
		_hardwareCounters()
	{
	}

	inline ~CPU()
	{
	}

	// Not copyable
	CPU(CPU const &) = delete;
	CPU operator=(CPU const &) = delete;

	inline bool initializeIfNeeded()
	{
		activation_status_t expectedStatus = uninitialized_status;
		bool worked = _activationStatus.compare_exchange_strong(expectedStatus, enabling_status);

		if (worked) {
			_threadingModelData.initialize(this);
			_instrumentationId = Instrument::createdCPU(_index, _NUMANodeId);
		} else {
			std::cout << "Initializing new CPU: old status was " << _activationStatus << "\n";
			assert(_activationStatus != enabling_status);
		}

		return worked;
	}

	//! \brief Force the initialization of this CPU
	//! NOTE: This method expects the status to already be initialized
	inline void initialize()
	{
		assert(_activationStatus != uninitialized_status);

		_threadingModelData.initialize(this);
		_instrumentationId = Instrument::createdCPU(_index, _NUMANodeId);
	}

	CPUThreadingModelData const &getThreadingModelData() const
	{
		return _threadingModelData;
	}

	CPUThreadingModelData &getThreadingModelData()
	{
		return _threadingModelData;
	}

	size_t getNumaNodeId() const
	{
		return _NUMANodeId;
	}

	size_t getSystemCPUId() const
	{
		return _systemCPUId;
	}

	std::atomic<activation_status_t> &getActivationStatus()
	{
		return _activationStatus;
	}

	cpu_set_t *getCpuMask()
	{
		return &_cpuMask;
	}

	pthread_attr_t *getPthreadAttr()
	{
		return &_pthreadAttr;
	}

	void setGroupId(size_t groupId)
	{
		_groupId = groupId;
	}

	size_t getGroupId() const
	{
		return _groupId;
	}

	inline CPUHardwareCounters &getHardwareCounters()
	{
		return _hardwareCounters;
	}

};


#endif // CPU_HPP

