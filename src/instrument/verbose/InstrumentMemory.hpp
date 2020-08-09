/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/


#ifndef INSTRUMENT_VERBOSE_MEMORY_HPP
#define INSTRUMENT_VERBOSE_MEMORY_HPP

#include "InstrumentLogMessage.hpp"
#include <InstrumentInstrumentationContext.hpp>
#include <InstrumentThreadInstrumentationContext.hpp>

#include "../api/InstrumentMemory.hpp"

namespace Instrument {

	template<typename T>
	struct instrument_allocator_t {

		std::atomic<std::size_t> _objectCounter;

		void allocatorInitialize()
		{
			_objectCounter = 0;
		}

		void allocatorShutdown()
		{
			if (_objectCounter > 0) {

				InstrumentationContext const &context = ThreadInstrumentationContext::getCurrent();

				LogEntry *logEntry = getLogEntry(context);
				assert(logEntry != nullptr);

				logEntry->appendLocation(context);

				logEntry->_contents << " Shutdown allocator for " << typeid(T).name()
					<< "leaked objects: " << _objectCounter;

				addLogEntry(logEntry) ;
			}
		}

		void allocateObject()
		{
			_objectCounter++;
		}

		void deallocateObject()
		{
			_objectCounter--;
		}

	};
}



#endif // INSTRUMENT_VERBOSE_MEMORY_HPP
