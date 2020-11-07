/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTERSHUTDOWNCALLBACK_HPP
#define CLUSTERSHUTDOWNCALLBACK_HPP

#include <cassert>

//! ShutdownCallback function to call during shutdown in the cases where
//! the runtime does not run the main function
class ClusterShutdownCallback {
	void (*_function)(void *);
	void *_args;
public:
	ClusterShutdownCallback(void (*func)(void *), void *args)
		: _function(func), _args(args)
	{
	}

	inline void execute()
	{
		assert(_function != nullptr);
		_function(_args);
	}
};

// They are the same and expected to remain the same because both are constructed with the arguments
// passed to spawn functions
typedef ClusterShutdownCallback ClusterTaskCallback;

#endif /* CLUSTERSHUTDOWNCALLBACK_HPP */
