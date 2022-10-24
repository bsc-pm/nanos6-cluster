/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include "RuntimeStateMonitor.hpp"
#include "tasks/Task.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "monitoring/MonitoringSupport.hpp"
#include "executors/threads/CPUManager.hpp"

std::vector<CPUStats *> RuntimeStateMonitor::_CPUStats;
CPUStatsTimer RuntimeStateMonitor::_intervalTimer;
SpinLock RuntimeStateMonitor::_lock;
bool RuntimeStateMonitor::_initialized = false;
busy_cores_t RuntimeStateMonitor::_totalBusyShutdown = 0;
busy_cores_t RuntimeStateMonitor::_usefulBusyShutdown = 0;
float RuntimeStateMonitor::_elapsed = 0;
std::vector<AveragedStats*> RuntimeStateMonitor::_listeners;
int RuntimeStateMonitor::_numCPUs;

AveragedStats::AveragedStats(double timePeriodSecs, bool onStats, bool totalBusy)
			: _timePeriodSecs(timePeriodSecs),
			_movingAverage(0),
			_onStats(onStats),
			_time(0),
			_totalBusy(totalBusy)
	{
		if (onStats) {
			RuntimeStateMonitor::pushBackListener(this);
		}
	}

AveragedStats::~AveragedStats()
{
	if (_onStats) {
		RuntimeStateMonitor::removeListener(this);
	}
}

void RuntimeStateMonitor::taskChangedStatus(Task *task, monitoring_task_status_t newStatus)
{
	(void)task;
	(void)newStatus;
	if (newStatus == ready_status) {
		return;
	}
	WorkerThread *thread = WorkerThread::getCurrentWorkerThread();
	assert(thread);
	CPU *cpu = thread->getComputePlace();

	updateTaskStatus(newStatus, cpu->getIndex());
}

void RuntimeStateMonitor::taskCompletedUserCode(__attribute__((unused)) Task *task)
{
	WorkerThread *thread = WorkerThread::getCurrentWorkerThread();
	assert(thread);
	CPU *cpu = thread->getComputePlace();
	updateTaskStatus(paused_status, cpu->getIndex());
}


void RuntimeStateMonitor::enterBlockCurrentTask(__attribute__((unused)) __attribute__((unused)) Task *task, __attribute__((unused)) bool fromUserCode)
{
	WorkerThread *thread = WorkerThread::getCurrentWorkerThread();
	assert(thread);
	CPU *cpu = thread->getComputePlace();
	updateTaskStatus(paused_status, cpu->getIndex());
}

void RuntimeStateMonitor::exitBlockCurrentTask(__attribute__((unused)) Task *task, __attribute__((unused)) bool fromUserCode)
{
	WorkerThread *thread = WorkerThread::getCurrentWorkerThread();
	assert(thread);
	CPU *cpu = thread->getComputePlace();
	updateTaskStatus(executing_status, cpu->getIndex());
}

void RuntimeStateMonitor::enterWaitFor(int virtualCPUId)
{
	updateTaskStatus(paused_status, virtualCPUId);
}

void RuntimeStateMonitor::exitWaitFor(int virtualCPUId)
{
	updateTaskStatus(executing_status, virtualCPUId);
}



void RuntimeStateMonitor::initialize()
{
	assert(!_initialized);

	_initialized = true;
	_totalBusyShutdown = 0;
	_usefulBusyShutdown = 0;
	_elapsed = 0;

	_numCPUs = CPUManager::getTotalCPUs();
	// std::cout << "RuntimeStateMonitor: #CPUs = " << _numCPUs << "\n";

	// Some timers start before initialize. Rather than making this an
	// error, and adding another dependency to the start up process, simply
	// clear any busy time already accumulated.
	CPUStatsTimer::Time endTime;
	CPUStatsTimer::getTime(endTime);
	std::lock_guard<SpinLock> guard(_lock);
	for(int cpu=0; cpu<_numCPUs; cpu++) {
		_CPUStats.push_back(new CPUStats());
	}
}

//! \brief Display CPU statistics
//!
//! \param[out] stream The output stream
void RuntimeStateMonitor::displayStatistics(std::stringstream &stream)
{
	stream << std::left << std::fixed << std::setprecision(2) << "\n";
	stream << "+-----------------------------+\n";
	stream << "|   RUNTIME STATE STATISTICS  |\n";
	stream << "+-----------------------------+\n";

	stream << "CPU       -  Inactive  Runtime  Running  AddTask    Idle\n";

	// Iterate through all CPUs and print their ID and activeness
	for (int id = 0; id < _numCPUs; ++id) {
		std::string label = "CPU(" + std::to_string(id) + ")";

		float times[CPUStats::State::numStates];
		RuntimeStateMonitor::getAllTimes(times, id);

		stream << std::setw(8) << label << " - ";
		float tot = 0;
		for(int state=0; state < CPUStats::State::numStates; state++) {
			stream << std::right << std::setw(8) << times[state] * 100.0 << std::left << "%";
			tot += times[state];
		}
		stream << "\n";
	}

	stream << "+-----------------------------+\n\n";
}
