/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_NULL_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP
#define INSTRUMENT_NULL_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP

#include "instrument/api/InstrumentDependencySubsystemEntryPoints.hpp"


namespace Instrument {


	inline void pushDependency(Stats::nanos6_dependency_state_t value)
	{
		Stats::nanos6_dependency_state_stats[value].fetch_add(1);
	}

	inline void enterRegisterTaskDataAcesses()
	{
		pushDependency(Stats::NANOS_REGISTERTASKDATAACCESSES);
	}

	inline void exitRegisterTaskDataAcesses() {}

	inline void enterUnregisterTaskDataAcesses()
	{
		pushDependency(Stats::NANOS_UNREGISTERTASKDATAACCESSES);
	}

	inline void exitUnregisterTaskDataAcesses() {}

	inline void enterPropagateSatisfiability()
	{
		pushDependency(Stats::NANOS_PROPAGATESATISFIABILITY);
	}

	inline void exitPropagateSatisfiability() {}

	inline void enterReleaseAccessRegion()
	{
		pushDependency(Stats::NANOS_RELEASEACCESSREGION);
	}

	inline void exitReleaseAccessRegion() {}

	inline void enterHandleEnterTaskwait()
	{
		pushDependency(Stats::NANOS_HANDLEENTERTASKWAIT);
	}

	inline void exitHandleEnterTaskwait() {}

	inline void enterHandleExitTaskwait()
	{
		pushDependency(Stats::NANOS_HANDLEEXITTASKWAIT);
	}

	inline void exitHandleExitTaskwait() {}

	inline void enterUnregisterTaskDataAcessesCallback()
	{
		pushDependency(Stats::NANOS_UNREGISTERTASKDATAACCESSESCALLBACK);
	}

	inline void enterUnregisterTaskDataAcesses2()
	{
		pushDependency(Stats::NANOS_UNREGISTERTASKDATAACCESSES2);
	}

	inline void exitUnregisterTaskDataAcesses2() {}

	inline void enterHandleCompletedTaskwaits()
	{
		pushDependency(Stats::NANOS_HANDLECOMPLETEDTASKWAITS);
	}

	inline void exitHandleCompletedTaskwaits() {}

	inline void enterSetupTaskwaitWorkflow()
	{
		pushDependency(Stats::NANOS_SETUPTASKWAITWORKFLOW);
	}

	inline void exitSetupTaskwaitWorkflow() {}

	inline void enterReleaseTaskwaitFragment()
	{
		pushDependency(Stats::NANOS_RELEASETASKWAITFRAGMENT);
	}

	inline void exitReleaseTaskwaitFragment() {}

	inline void enterCreateDataCopyStep(bool isTaskwait)
	{
		if (isTaskwait) {
			pushDependency(Stats::NANOS_CREATEDATACOPYSTEP_TASKWAIT);
		} else {
			pushDependency(Stats::NANOS_CREATEDATACOPYSTEP_TASK);
		}
	}

	inline void exitCreateDataCopyStep(__attribute__((unused)) bool isTaskwait) {}
}

#endif //INSTRUMENT_NULL_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP

