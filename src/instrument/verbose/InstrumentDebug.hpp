/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_VERBOSE_DEBUG_HPP
#define INSTRUMENT_VERBOSE_DEBUG_HPP

#include <cstdint>

#include "InstrumentVerbose.hpp"

#include "instrument/api/InstrumentDebug.hpp"

namespace Instrument {

	inline void debugEnter(
		__attribute__((unused)) uint8_t id
	) {
	}

	inline void debugTransition(
		__attribute__((unused)) uint8_t id
	) {
	}

	inline void debugExit() {}

	inline void debugRegister(
		__attribute__((unused)) const char *name,
		__attribute__((unused)) uint8_t id
	) {
	}

	inline void emitInstrumentationEvent(size_t type, size_t value)
	{
		InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent();
		Verbose::LogEntry *logEntry = Verbose::getLogEntry(context);

		assert(logEntry != nullptr);
		logEntry->appendLocation(context);

		logEntry->_contents << " User event: " << type <<":"<< value << ": ";
		Verbose::addLogEntry(logEntry);
	}
}

#endif //INSTRUMENT_VERBOSE_DEBUG_HPP

