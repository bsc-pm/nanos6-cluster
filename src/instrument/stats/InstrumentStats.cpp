/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include "InstrumentStats.hpp"

#include <iostream>
#include <iomanip>      // std::setw
#include <string>

namespace Instrument {
	namespace Stats {
		RWTicketSpinLock _phasesSpinLock;
		int _currentPhase(0);
		std::vector<Timer> _phaseTimes;

		SpinLock _threadInfoListSpinLock;
		std::list<ThreadInfo *> _threadInfoList;

		Timer _totalTime(true);

		std::array<std::atomic<size_t>, NANOS_DEPENDENCY_STATE_TYPES>
			nanos6_dependency_state_stats = {};

		void show_dependency_state_stats(std::ostream &output)
		{
			std::array<std::string, NANOS_DEPENDENCY_STATE_TYPES> dependency_state_names =
				{
					#define HELPER(arg) #arg,
					NANOS6_DEPENDENCY_STATE_MACRO
					#undef HELPER
				};

			for (size_t type = 0; type < NANOS_DEPENDENCY_STATE_TYPES; ++type) {
				output << "# STATE\t"
					<< std::left << std::setw(48) << dependency_state_names[type]
					<< std::setw(0) << std::right << "\t" << nanos6_dependency_state_stats[type]
					<< std::endl;
			}
		}

	}
}
