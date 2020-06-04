/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef PAPI_TASK_HARDWARE_COUNTERS_HPP
#define PAPI_TASK_HARDWARE_COUNTERS_HPP

#include "hardware-counters/TaskHardwareCountersInterface.hpp"


class PAPITaskHardwareCounters : public TaskHardwareCountersInterface {

public:

	PAPITaskHardwareCounters(bool = true)
	{
	}

	//! \brief Empty hardware counter structures
	inline void clear() override
	{
	}

	//! \brief Get the delta value of a HW counter
	//!
	//! \param[in] counterId The type of counter to get the delta from
	inline double getDelta(HWCounters::counters_t) override
	{
		return 0.0;
	}

	//! \brief Get the accumulated value of a HW counter
	//!
	//! \param[in] counterId The type of counter to get the accumulation from
	inline double getAccumulated(HWCounters::counters_t) override
	{
		return 0.0;
	}

};

#endif // PAPI_TASK_HARDWARE_COUNTERS_HPP
