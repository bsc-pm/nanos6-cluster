/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTERSHUTDOWNCALLBACK_HPP
#define CLUSTERSHUTDOWNCALLBACK_HPP

#include "system/ompss/SpawnFunction.hpp"

#include <cassert>

//! ShutdownCallback function to call during shutdown in the cases where
//! the runtime does not run the main function
class ClusterShutdownCallback {
	SpawnFunction::function_t _function;
	void *_args;
	std::atomic<size_t> _counter;
public:
	ClusterShutdownCallback(void (*func)(void *), void *args)
		: _function(func), _args(args), _counter(0)
	{
	}

	~ClusterShutdownCallback()
	{
		assert(_counter.load() == 0);
	}

	inline void execute()
	{
		assert(_counter.load() == 0);
		if (_function != nullptr) {
			_function(_args);
		}
	}

	inline size_t increment(size_t val = 1)
	{
		assert(val > 0);
		return _counter += val;
	}

	inline size_t decrement(size_t val = 1)
	{
		assert(val > 0);
		assert(_counter.load() >= val);

		if ((_counter -= val) == 0) {
			this->execute();
		}

		return _counter.load();
	}

	inline size_t getCounterValue() const
	{
		return _counter.load();
	}

};

// They are the same and expected to remain the same because both are constructed with the arguments
// passed to spawn functions
typedef ClusterShutdownCallback ClusterTaskCallback;

#endif /* CLUSTERSHUTDOWNCALLBACK_HPP */
