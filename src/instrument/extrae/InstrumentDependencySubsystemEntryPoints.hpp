/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_EXTRAE_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP
#define INSTRUMENT_EXTRAE_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP

#include "instrument/api/InstrumentDependencySubsystemEntryPoints.hpp"
#include "InstrumentExtrae.hpp"
#include "InstrumentThreadLocalData.hpp"

namespace Instrument {


	inline void pushDependency(extrae_value_t value)
	{
		ThreadLocalData &threadLocal = getThreadLocalData();
		threadLocal._dependencyNesting.push_back(value);
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, value);
	}

	inline void popDependency(__attribute__((unused)) extrae_value_t value)
	{
		ThreadLocalData &threadLocal = getThreadLocalData();
		assert(!threadLocal._dependencyNesting.empty());
		assert(threadLocal._dependencyNesting.back() == value);
		threadLocal._dependencyNesting.pop_back();
		if (threadLocal._dependencyNesting.empty()) {
			emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, 0);
		} else {
			emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, threadLocal._dependencyNesting.back());
		}
	}

	inline void enterRegisterTaskDataAcesses()
	{
		pushDependency(NANOS_REGISTERTASKDATAACCESSES);
	}

	inline void exitRegisterTaskDataAcesses()
	{
		popDependency(NANOS_REGISTERTASKDATAACCESSES);
	}

	inline void enterUnregisterTaskDataAcesses()
	{
		pushDependency(NANOS_UNREGISTERTASKDATAACCESSES);
	}

	inline void exitUnregisterTaskDataAcesses()
	{
		popDependency(NANOS_UNREGISTERTASKDATAACCESSES);
	}

	inline void enterPropagateSatisfiability()
	{
		pushDependency(NANOS_PROPAGATESATISFIABILITY);
	}

	inline void exitPropagateSatisfiability()
	{
		popDependency(NANOS_PROPAGATESATISFIABILITY);
	}

	inline void enterReleaseAccessRegion()
	{
		pushDependency(NANOS_RELEASEACCESSREGION);
	}

	inline void exitReleaseAccessRegion()
	{
		popDependency(NANOS_RELEASEACCESSREGION);
	}

	inline void enterHandleEnterTaskwait()
	{
		pushDependency(NANOS_HANDLEENTERTASKWAIT);
	}

	inline void exitHandleEnterTaskwait()
	{
		popDependency(NANOS_HANDLEENTERTASKWAIT);
	}

	inline void enterHandleExitTaskwait()
	{
		pushDependency(NANOS_HANDLEEXITTASKWAIT);
	}

	inline void exitHandleExitTaskwait()
	{
		popDependency(NANOS_HANDLEEXITTASKWAIT);
	}

	inline void enterUnregisterTaskDataAcessesCallback()
	{
		popDependency(NANOS_UNREGISTERTASKDATAACCESSES);
		pushDependency(NANOS_UNREGISTERTASKDATAACCESSESCALLBACK);
	}

	inline void enterUnregisterTaskDataAcesses2()
	{
		popDependency(NANOS_UNREGISTERTASKDATAACCESSESCALLBACK);
		pushDependency(NANOS_UNREGISTERTASKDATAACCESSES2);
	}

	inline void exitUnregisterTaskDataAcesses2()
	{
		popDependency(NANOS_UNREGISTERTASKDATAACCESSES2);
	}

	inline void enterHandleCompletedTaskwaits()
	{
		pushDependency(NANOS_HANDLECOMPLETEDTASKWAITS);
	}

	inline void exitHandleCompletedTaskwaits()
	{
		popDependency(NANOS_HANDLECOMPLETEDTASKWAITS);
	}

	inline void enterSetupTaskwaitWorkflow()
	{
		pushDependency(NANOS_SETUPTASKWAITWORKFLOW);
	}

	inline void exitSetupTaskwaitWorkflow()
	{
		popDependency(NANOS_SETUPTASKWAITWORKFLOW);
	}

	inline void enterReleaseTaskwaitFragment()
	{
		pushDependency(NANOS_RELEASETASKWAITFRAGMENT);
	}

	inline void exitReleaseTaskwaitFragment()
	{
		popDependency(NANOS_RELEASETASKWAITFRAGMENT);
	}
}

#endif //INSTRUMENT_EXTRAE_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP

