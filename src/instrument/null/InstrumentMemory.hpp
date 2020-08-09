/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/


#ifndef INSTRUMENT_NULL_MEMORY_HPP
#define INSTRUMENT_NULL_MEMORY_HPP

#include "../api/InstrumentMemory.hpp"

namespace Instrument {

	template<typename T>
	struct instrument_allocator_t {

		inline void allocatorInitialize()
		{}

		inline void allocatorShutdown()
		{}

		inline void allocateObject()
		{}

		inline void deallocateObject()
		{}
	};
}



#endif // INSTRUMENT_VERBOSE_MEMORY_HPP
