/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2019 Barcelona Supercomputing Center (BSC)
*/

#include "Scheduler.hpp"
#include "system/RuntimeInfo.hpp"

#ifdef USE_CLUSTER
#include "ClusterScheduler.hpp"
typedef ClusterScheduler instanceScheduler;
#else
#include "LocalScheduler.hpp"
typedef LocalScheduler instanceScheduler;
#endif


SchedulerInterface *Scheduler::_instance;

void Scheduler::initialize()
{
	_instance = new instanceScheduler();

	assert(_instance != nullptr);
	RuntimeInfo::addEntry("scheduler", "Scheduler", _instance->getName());
}

void Scheduler::shutdown()
{
	delete _instance;
}
