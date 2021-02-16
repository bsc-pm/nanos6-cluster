/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2019 Barcelona Supercomputing Center (BSC)
*/

#include "Scheduler.hpp"
#include "system/RuntimeInfo.hpp"

#ifdef USE_CLUSTER
#include "ClusterScheduler.hpp"
#else
#include "LocalScheduler.hpp"
#endif


SchedulerInterface *Scheduler::_instance = nullptr;

void Scheduler::initialize()
{
#ifdef USE_CLUSTER
	ConfigVariable<std::string> clusterSchedulerName("cluster.scheduling_policy");

	_instance = ClusterScheduler::generate(clusterSchedulerName.getValue());
#else
	_instance = new LocalScheduler();
#endif

	assert(_instance != nullptr);
	RuntimeInfo::addEntry("scheduler", "Scheduler", _instance->getName());
}

void Scheduler::shutdown()
{
	delete _instance;
	_instance = nullptr;
}
