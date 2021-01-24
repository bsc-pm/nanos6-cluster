/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_EXTRAE_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP
#define INSTRUMENT_EXTRAE_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP

#include "instrument/api/InstrumentDependencySubsystemEntryPoints.hpp"
#include "InstrumentExtrae.hpp"

namespace Instrument {


	inline void enterRegisterTaskDataAcesses()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, NANOS_REGISTERTASKDATAACCESSES);
	}

	inline void exitRegisterTaskDataAcesses()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, 0);
	}

	inline void enterUnregisterTaskDataAcesses()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, NANOS_UNREGISTERTASKDATAACCESSES);
	}

	inline void exitUnregisterTaskDataAcesses()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, 0);
	}

	inline void enterPropagateSatisfiability()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, NANOS_PROPAGATESATISFIABILITY);
	}

	inline void exitPropagateSatisfiability()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, 0);
	}

	inline void enterReleaseAccessRegion()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, NANOS_RELEASEACCESSREGION);
	}

	inline void exitReleaseAccessRegion()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, 0);
	}

	inline void enterHandleEnterTaskwait()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, NANOS_HANDLEENTERTASKWAIT);
	}

	inline void exitHandleEnterTaskwait()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, 0);
	}

	inline void enterHandleExitTaskwait()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, NANOS_HANDLEEXITTASKWAIT);
	}

	inline void exitHandleExitTaskwait()
	{
		emitEvent(EventType::DEPENDENCIES_SUBSYSTEM, 0);
	}

}

#endif //INSTRUMENT_EXTRAE_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP

