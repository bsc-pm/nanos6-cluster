/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include <cassert>
#include <ctime>
#include <iostream>
#include <time.h>

#include "LeaderThread.hpp"
#include "PollingAPI.hpp"
#include "support/config/ConfigVariable.hpp"
#include "executors/threads/CPUManager.hpp"

#include <InstrumentLeaderThread.hpp>
#include <InstrumentThreadManagement.hpp>


LeaderThread *LeaderThread::_singleton = nullptr;

void LeaderThread::body()
{
	assert(_singleton != nullptr);
	initializeHelperThread();
	Instrument::threadHasResumed(getInstrumentationId());
	// Minimum polling interval in microseconds
	ConfigVariable<int> pollingFrequency("misc.polling_frequency");

	while (!std::atomic_load_explicit(&_mustExit, std::memory_order_relaxed)) {

		if (!CPUManager::hasReservedCPUforLeaderThread()) {
			// In cluster mode there is a dedicated thread for the LeaderThread.
			// It is therefore not necessary for it to sleep. Only sleep in
			// non-cluster mode.
			struct timespec delay = {0, pollingFrequency * 1000};
			// The loop repeats the call with the remaining time in the event that
			// the thread received a signal with a handler that has SA_RESTART set
			Instrument::threadWillSuspend(getInstrumentationId());
			while (nanosleep(&delay, &delay)) {
			}
			Instrument::threadHasResumed(getInstrumentationId());
		}

		PollingAPI::handleServices();

		Instrument::leaderThreadSpin();
	}

	Instrument::threadWillShutdown(getInstrumentationId());

	return;
}
