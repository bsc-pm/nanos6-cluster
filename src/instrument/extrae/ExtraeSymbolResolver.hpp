/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2017 Barcelona Supercomputing Center (BSC)
*/

#ifndef EXTRAE_SYMBOL_RESOLVER_HPP
#define EXTRAE_SYMBOL_RESOLVER_HPP


#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <mutex>
#include <cassert>
#include <dlfcn.h>

#include <string>

#include <support/StringLiteral.hpp>
#include "lowlevel/SpinLock.hpp"
#include "lowlevel/PaddedTicketSpinLock.hpp"


class ExtraeSymbolResolverBase {
protected:
	void *_handle;
	static SpinLock _lock;
	
	static ExtraeSymbolResolverBase *_singleton;
	
	static inline void *getHandle()
	{
		assert(_singleton);
		return _singleton->_handle;
	}
	
public:
	ExtraeSymbolResolverBase();

	// Create the singleton with an explicit initialize call,
	// which must be called in the instrumentation initialization
	// after the initialization of Nanos6@cluster. This is because
	// the choice of Extrae library depends on whether cluster mode
	// is enabled.
	static void initialize()
	{
		_singleton = new ExtraeSymbolResolverBase;
	}
	
	static std::string getSharedObjectPath();
};


template <typename RETURN_T, StringLiteral const *NAME, typename... PARAMETERS_T>
class ExtraeSymbolResolver : public ExtraeSymbolResolverBase {
private:
	
public:
	typedef RETURN_T (*function_t)(PARAMETERS_T...);
	
	static RETURN_T call(PARAMETERS_T... parameters)
	{
		std::lock_guard<SpinLock> guard(_lock);
		static function_t symbol = (function_t) dlsym(ExtraeSymbolResolverBase::getHandle(), *NAME);
		assert(symbol != nullptr);
		
		return (*symbol)(parameters...);
	}
	
	static function_t getFunction()
	{
		static function_t symbol = (function_t) dlsym(ExtraeSymbolResolverBase::getHandle(), *NAME);
		assert(symbol != nullptr);
		
		return symbol;
	}
	
	static void *getSymbol()
	{
		static void *symbol = dlsym(ExtraeSymbolResolverBase::getHandle(), *NAME);
		assert(symbol != nullptr);
		
		return symbol;
	}
};


#endif // EXTRAE_SYMBOL_RESOLVER_HPP
