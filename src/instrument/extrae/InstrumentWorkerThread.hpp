/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_EXTRAE_WORKERTHREAD_HPP
#define INSTRUMENT_EXTRAE_WORKERTHREAD_HPP

#include <cassert>

#include "instrument/api/InstrumentWorkerThread.hpp"
#include "instrument/extrae/InstrumentThreadManagement.hpp"
#include "instrument/support/InstrumentThreadLocalDataSupport.hpp"

namespace Instrument {

	inline void workerThreadSpins()
	{
	}

	inline void workerThreadObtainedTask()
	{
		ThreadLocalData &tld = getThreadLocalData();
		if (tld.isBusyWaiting) {
			tld.isBusyWaiting = false;
			exitBusyWait();
		}
	}

	inline void workerThreadBusyWaits()
	{
		ThreadLocalData &tld = getThreadLocalData();
		if (!tld.isBusyWaiting) {
			tld.isBusyWaiting = true;
			enterBusyWait();
		}
	}
}

#endif // INSTRUMENT_EXTRAE_WORKERTHREAD_HPP

