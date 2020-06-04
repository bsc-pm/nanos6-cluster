/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CPU_HARDWARE_COUNTERS_HPP
#define CPU_HARDWARE_COUNTERS_HPP

#include "CPUHardwareCountersInterface.hpp"
#include "HardwareCounters.hpp"
#include "SupportedHardwareCounters.hpp"

#if HAVE_PAPI
#include "hardware-counters/papi/PAPICPUHardwareCounters.hpp"
#endif

#if HAVE_PQOS
#include "hardware-counters/pqos/PQoSCPUHardwareCounters.hpp"
#endif


class CPUHardwareCounters {

private:

	//! CPU-related hardware counters for the PAPI backend
	CPUHardwareCountersInterface *_papiCounters;

	//! CPU-related hardware counters for the PQoS backend
	CPUHardwareCountersInterface *_pqosCounters;

public:

	inline CPUHardwareCounters() :
		_papiCounters(nullptr),
		_pqosCounters(nullptr)
	{
#if HAVE_PAPI
		if (HardwareCounters::isBackendEnabled(HWCounters::PAPI_BACKEND)) {
			_papiCounters = new PAPICPUHardwareCounters();
		}
#endif

#if HAVE_PQOS
		if (HardwareCounters::isBackendEnabled(HWCounters::PQOS_BACKEND)) {
			_pqosCounters = new PQoSCPUHardwareCounters();
		}
#endif
	}


	inline ~CPUHardwareCounters()
	{
		if (_papiCounters != nullptr) {
			delete _papiCounters;
		}

		if (_pqosCounters != nullptr) {
			delete _pqosCounters;
		}
	}

	//! \brief Return the PAPI counters of the CPU (if it is enabled) or nullptr
	inline CPUHardwareCountersInterface *getPAPICounters() const
	{
		return _papiCounters;
	}

	//! \brief Return the PQOS counters of the cpu (if it is enabled) or nullptr
	inline CPUHardwareCountersInterface *getPQoSCounters() const
	{
		return _pqosCounters;
	}

	//! \brief Get the delta value of a HW counter
	//!
	//! \param[in] counterId The type of counter to get the delta from
	inline double getDelta(HWCounters::counters_t counterId)
	{
		CPUHardwareCountersInterface *cpuCounters = nullptr;
		if (counterId >= HWCounters::PQOS_MIN_EVENT && counterId <= HWCounters::PQOS_MAX_EVENT) {
			cpuCounters = getPQoSCounters();
		} else if (counterId >= HWCounters::PAPI_MIN_EVENT && counterId <= HWCounters::PAPI_MAX_EVENT) {
			cpuCounters = getPAPICounters();
		}
		assert(cpuCounters != nullptr);

		return cpuCounters->getDelta(counterId);
	}

};

#endif // CPU_HARDWARE_COUNTERS_HPP
