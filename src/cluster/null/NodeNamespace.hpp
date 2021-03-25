/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/


#ifndef NODENAMESPACE_H
#define NODENAMESPACE_H

#include <deque>
#include <unistd.h>

#include "ClusterManager.hpp"
#include "tasks/Task.hpp"

class NodeNamespace {
public:

	// static void init(void (*func)(void *), void *args)
	// {}

	static inline void callbackIncrement()
	{}

	static inline void callbackDecrement()
	{}

	static inline bool isEnabled()
	{
		return false;
	}
};


#endif /* NODENAMESPACE_H */
