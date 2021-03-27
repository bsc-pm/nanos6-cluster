/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_EXTRAE_DEBUG_HPP
#define INSTRUMENT_EXTRAE_DEBUG_HPP

#include <cstdint>

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

	inline void emitInstrumentationEvent(size_t event, size_t value)
	{
		ExtraeAPI::event((extrae_type_t)event, (extrae_value_t) value);
	}

}

#endif //INSTRUMENT_EXTRAE_DEBUG_HPP

