/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2017 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_VERBOSE_TASK_EXECUTION_HPP
#define INSTRUMENT_VERBOSE_TASK_EXECUTION_HPP


#include "instrument/api/InstrumentTaskExecution.hpp"

namespace Instrument {

	inline void startTaskforCollaborator(
		__attribute__((unused)) task_id_t taskforId,
		__attribute__((unused)) task_id_t collaboratorId,
		__attribute__((unused))  bool first,
		__attribute__((unused)) InstrumentationContext const &context)
	{
		// Verbose instrumentation does not instrument taskfor
	}


	inline void endTaskforCollaborator(
		__attribute__((unused)) task_id_t taskforId,
		__attribute__((unused)) task_id_t collaboratorId,
		__attribute__((unused)) bool last,
		__attribute__((unused)) InstrumentationContext const &context)
	{
		// Verbose instrumentation does not instrument
	}

	inline void taskforChunk(__attribute__((unused)) int chunk)
	{
		// Verbose instrumentation does not instrument taskfor
	}


}

#endif // INSTRUMENT_VERBOSE_TASK_EXECUTION_HPP
