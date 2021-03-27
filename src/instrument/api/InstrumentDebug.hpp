/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_POLLING_SERVICES_HPP
#define INSTRUMENT_POLLING_SERVICES_HPP

#include <cstdint>

namespace Instrument {

	void pollingServiceEnter(uint8_t id);
	void pollingServiceExit();
	inline void pollingServiceRegister(const char *name, uint8_t id);

	void emitInstrumentationEvent(size_t event, size_t value);
}

#endif //INSTRUMENT_POLLING_SERVICES_HPP
