/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP
#define INSTRUMENT_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP


namespace Instrument {

	//! \brief Enters task registration
	void enterRegisterTaskDataAcesses();

	//! \brief Exists task registration
	void exitRegisterTaskDataAcesses();

	//! \brief Enter task unregistration
	void enterUnregisterTaskDataAcesses();

	//! \brief Exit task unregistration
	void exitUnregisterTaskDataAcesses();

	void enterPropagateSatisfiability();

	void exitPropagateSatisfiability();

	void enterReleaseAccessRegion();

	void exitReleaseAccessRegion();

	void enterHandleEnterTaskwait();

	void exitHandleEnterTaskwait();

	void enterHandleExitTaskwait();

	void exitHandleExitTaskwait();

	void enterUnregisterTaskDataAcessesCallback();

	void enterUnregisterTaskDataAcesses2();

	void exitUnregisterTaskDataAcesses2();

	void enterHandleCompletedTaskwaits();

	void exitHandleCompletedTaskwaits();

	void enterSetupTaskwaitWorkflow();

	void exitSetupTaskwaitWorkflow();
}

#endif //INSTRUMENT_DEPENDENCY_SUBSYTEM_ENTRY_POINTS_HPP
