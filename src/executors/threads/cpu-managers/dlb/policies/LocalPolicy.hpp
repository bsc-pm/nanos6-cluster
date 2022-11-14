/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef LOCAL_POLICY_HPP
#define LOCAL_POLICY_HPP

#include "executors/threads/CPUManagerPolicyInterface.hpp"
#include "hardware/places/ComputePlace.hpp"
#include "monitoring/RuntimeStateMonitor.hpp"


class LocalPolicy : public CPUManagerPolicyInterface {

private:

	//! The maximum amount of CPUs in the system
	size_t _numCPUs;
	AveragedStats *_averagedStats;

public:

	LocalPolicy(size_t numCPUs);

	void execute(ComputePlace *cpu, CPUManagerPolicyHint hint, size_t numRequested = 0);

};

#endif // LOCAL_POLICY_HPP
