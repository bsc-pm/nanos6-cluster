/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_NULL_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP
#define INSTRUMENT_NULL_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP

#include "instrument/api/InstrumentDependencySubsystemEntryPoints.hpp"


namespace Instrument {

	inline void enterRegisterTaskDataAcesses() {}

	inline void exitRegisterTaskDataAcesses() {}

	inline void enterUnregisterTaskDataAcesses() {}

	inline void exitUnregisterTaskDataAcesses() {}

	inline void enterPropagateSatisfiability() {}

	inline void exitPropagateSatisfiability() {}

	inline void enterReleaseAccessRegion() {}

	inline void exitReleaseAccessRegion() {}

	inline void enterHandleEnterTaskwait() {}

	inline void exitHandleEnterTaskwait() {}

	inline void enterHandleExitTaskwait() {}

	inline void exitHandleExitTaskwait() {}

	inline void enterUnregisterTaskDataAcessesCallback() {}

	inline void enterUnregisterTaskDataAcesses2() {}

	inline void exitUnregisterTaskDataAcesses2() {}

	inline void enterHandleCompletedTaskwaits() {}

	inline void exitHandleCompletedTaskwaits() {}

	inline void enterSetupTaskwaitWorkflow() {}

	inline void exitSetupTaskwaitWorkflow() {}

	inline void enterReleaseTaskwaitFragment() {}

	inline void exitReleaseTaskwaitFragment() {}

	inline void enterCreateDataCopyStep(__attribute__((unused)) bool isTaskwait) {}

	inline void exitCreateDataCopyStep(__attribute__((unused)) bool isTaskwait) {}

	inline void emitDependencyUserEvent(__attribute__((unused)) size_t eventValue = 0) {}

	inline void enterTaskDataAccessLocation() {}

	inline void exitTaskDataAccessLocation() {}

	inline void enterProcessDelayedOperationsSatisfiedOriginatorsAndRemovableTasks() {};

	inline void exitProcessDelayedOperationsSatisfiedOriginatorsAndRemovableTasks() {};
}

#endif //INSTRUMENT_NULL_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP

