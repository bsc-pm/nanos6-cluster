/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include <cassert>

#include <nanos6/debug.h>

#include "executors/threads/CPU.hpp"
#include "executors/threads/CPUManager.hpp"
#include "executors/threads/ThreadManager.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "tasks/Task.hpp"
#include "tasks/TaskImplementation.hpp"

#include <InstrumentDebug.hpp>


void nanos6_wait_for_full_initialization(void)
{
	while (!CPUManager::hasFinishedInitialization()) {
		// Wait
	}
}

unsigned int nanos6_get_num_cpus(void)
{
	return CPUManager::getAvailableCPUs();
}

unsigned int nanos6_get_total_num_cpus(void)
{
	return CPUManager::getTotalCPUs();
}

long nanos6_get_current_system_cpu(void)
{
	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	if (currentThread == nullptr) {
		return 0;
	}

	CPU *currentCPU = currentThread->getComputePlace();
	assert(currentCPU != 0);

	return currentCPU->getSystemCPUId();
}

unsigned int nanos6_get_current_virtual_cpu(void)
{
	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	if (currentThread == nullptr) {
		return 0;
	}

	CPU *currentCPU = currentThread->getComputePlace();
	assert(currentCPU != 0);

	return currentCPU->getIndex();
}

int nanos6_enable_cpu(long systemCPUId)
{
	return CPUManager::enable(systemCPUId);
}

int nanos6_disable_cpu(long systemCPUId)
{
	return CPUManager::disable(systemCPUId);
}

nanos6_cpu_status_t nanos6_get_cpu_status(long systemCPUId)
{
	CPU *cpu = CPUManager::getCPU(systemCPUId);
	assert(cpu != nullptr);

	switch (cpu->getActivationStatus().load()) {
		case CPU::uninitialized_status:
		case CPU::uninitialized_given_status:
			return nanos6_uninitialized_cpu;
		case CPU::enabled_status:
			return nanos6_enabled_cpu;
		case CPU::enabling_status:
			return nanos6_enabling_cpu;
		case CPU::disabled_status:
			return nanos6_disabled_cpu;
		case CPU::disabling_status:
			return nanos6_disabling_cpu;
		case CPU::lent_status:
			return nanos6_lent_cpu;
		case CPU::lending_status:
			return nanos6_lending_cpu;
		case CPU::acquired_status:
			return nanos6_acquired_cpu;
		case CPU::acquired_enabled_status:
			return nanos6_acquired_enabled_cpu;
		case CPU::returned_status:
			return nanos6_returned_cpu;
		case CPU::shutting_down_status:
			return nanos6_shutting_down_cpu;
		case CPU::shutdown_status:
			return nanos6_shutdown_cpu;
		case CPU::giving_status:
			return nanos6_giving_cpu;
	}

	assert("Unknown CPU status" == 0);
	return nanos6_invalid_cpu_status;
}

#if 0
void nanos6_wait_until_task_starts(void *taskHandle)
{
	assert(taskHandle != 0);

	Task *task = (Task *) taskHandle;
	while (task->getThread() == 0) {
		// Wait
	}
}


long nanos6_get_system_cpu_of_task(void *taskHandle)
{
	assert(taskHandle != 0);

	Task *task = (Task *) taskHandle;
	WorkerThread *thread = task->getThread();

	assert(thread != 0);
	CPU *cpu = thread->getComputePlace();

	assert(cpu != 0);
	return cpu->getSystemCPUId();
}
#endif

typedef std::vector<CPU *>::const_iterator cpu_iterator_t;

static void *nanos6_cpus_skip_uninitialized(void *cpuIterator) {
	std::vector<CPU *> const &cpuList = CPUManager::getCPUListReference();

	cpu_iterator_t *itp = (cpu_iterator_t *) cpuIterator;
	if (itp == 0) {
		return 0;
	}

	do {
		if ((*itp) == cpuList.end()) {
			delete itp;
			return 0;
		}

		CPU *cpu = *(*itp);

		if ((cpu != 0) && (cpu->getActivationStatus() != CPU::uninitialized_status)) {
			return itp;
		}

		(*itp)++;
	} while (true);
}


void *nanos6_cpus_begin(void)
{
	std::vector<CPU *> const &cpuList = CPUManager::getCPUListReference();
	cpu_iterator_t it = cpuList.begin();

	if (it == cpuList.end()) {
		return 0;
	} else {
		void *result = new cpu_iterator_t(it);
		return nanos6_cpus_skip_uninitialized(result);
	}
}

void *nanos6_cpus_end(void)
{
	return 0;
}

void *nanos6_cpus_advance(void *cpuIterator)
{
	cpu_iterator_t *itp = (cpu_iterator_t *) cpuIterator;
	if (itp == 0) {
		return 0;
	}

	(*itp)++;
	return nanos6_cpus_skip_uninitialized(itp);
}

long nanos6_cpus_get(void *cpuIterator)
{
	cpu_iterator_t *it = (cpu_iterator_t *) cpuIterator;
	assert (it != 0);

	CPU *cpu = *(*it);
	assert(cpu != 0);

	return cpu->getSystemCPUId();
}

long nanos6_cpus_get_virtual(void *cpuIterator)
{
	cpu_iterator_t *it = (cpu_iterator_t *) cpuIterator;
	assert (it != 0);

	CPU *cpu = *(*it);
	assert(cpu != 0);

	return cpu->getIndex();
}

long nanos6_cpus_get_numa(void *cpuIterator)
{
	cpu_iterator_t *it = (cpu_iterator_t *) cpuIterator;
	assert (it != 0);

	CPU *cpu = *(*it);
	assert(cpu != 0);

	return cpu->getNumaNodeId();
}


long nanos6_get_current_numa(void)
{
	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	if (currentThread == nullptr) {
		return -1;
	}

	CPU *currentCPU = currentThread->getComputePlace();
	assert(currentCPU != 0);

	return currentCPU->getNumaNodeId();
}

int nanos6_is_dlb_enabled(void)
{
	return CPUManager::isDLBEnabled();
}


void nanos6_instrument_event(unsigned int event, unsigned int value)
{
	// Invert this value.
	Instrument::emitInstrumentationEvent(event, value);
}
