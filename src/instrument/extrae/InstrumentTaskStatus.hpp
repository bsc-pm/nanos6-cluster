/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_EXTRAE_TASK_STATUS_HPP
#define INSTRUMENT_EXTRAE_TASK_STATUS_HPP


#include "instrument/api/InstrumentTaskStatus.hpp"
#include "InstrumentExtrae.hpp"


namespace Instrument {
	inline void taskIsPending(
		__attribute__((unused)) task_id_t taskId,
		__attribute__((unused)) InstrumentationContext const &context
	) {
	}

	inline void taskIsReady(
		__attribute__((unused)) task_id_t taskId,
		__attribute__((unused)) InstrumentationContext const &context
	) {
		// Non-precise task count (sampled)
		if (!Extrae::_detailTaskCount) {
			return;
		}

		extrae_combined_events_t ce;

		ce.HardwareCounters = 1;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;

		ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
		ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

		ce.Types[0] = (extrae_type_t) EventType::READY_TASKS;
		ce.Values[0] = (extrae_value_t) ++_readyTasks;

		// This counter is not so reliable, so try to skip underflows
		if (((signed long long) ce.Values[0]) < 0) {
			ce.Values[0] = 0;
		}

		Extrae::emit_CombinedEvents ( &ce );
	}

	inline void taskIsExecuting(
		__attribute__((unused)) task_id_t taskId,
		__attribute__((unused)) bool wasBlocked,
		__attribute__((unused)) InstrumentationContext const &context
	) {
	}

	inline void taskIsBlocked(
		__attribute__((unused)) task_id_t taskId,
		__attribute__((unused)) task_blocking_reason_t reason,
		__attribute__((unused)) InstrumentationContext const &context
	) {
	}

	inline void taskIsZombie(
		__attribute__((unused)) task_id_t taskId,
		__attribute__((unused)) InstrumentationContext const &context
	) {
	}

	inline void taskIsBeingDeleted(
		task_id_t taskId,
		__attribute__((unused)) InstrumentationContext const &context
	) {
		delete(taskId._taskInfo);
		taskId._taskInfo = nullptr;
	}

	inline void taskHasNewPriority(
		task_id_t taskId,
		long priority,
		__attribute__((unused)) InstrumentationContext const &context
	) {
		taskId._taskInfo->_priority = priority;
	}

	inline void taskforCollaboratorIsExecuting(
		__attribute__((unused)) task_id_t taskforId,
		__attribute__((unused)) task_id_t collaboratorId,
		__attribute__((unused)) InstrumentationContext const &context
	) {
	}

	inline void taskforCollaboratorStopped(
		__attribute__((unused)) task_id_t taskId,
		__attribute__((unused)) task_id_t taskforId,
		__attribute__((unused)) InstrumentationContext const &context
	) {
	}

	inline void taskNUMAHint(
		__attribute__((unused)) task_id_t taskforId,
		__attribute__((unused)) int64_t numaHint,
		__attribute__((unused)) InstrumentationContext const &context
	) {
		extrae_combined_events_t ce;

		ce.HardwareCounters = 1;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;

		ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
		ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

		ce.Types[0] = (extrae_type_t) EventType::NUMA_HINT;
		ce.Values[0] = numaHint;

		Extrae::emit_CombinedEvents ( &ce );
	}

}


#endif // INSTRUMENT_EXTRAE_TASK_STATUS_HPP
