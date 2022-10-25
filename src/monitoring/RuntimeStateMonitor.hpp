/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef RUNTIMESTATEMONITOR_HPP
#define RUNTIMESTATEMONITOR_HPP

#include <algorithm>
#include <math.h>
#include <time.h>
#include <unordered_map>
#include "ClusterManager.hpp"
#include "lowlevel/SpinLock.hpp"
#include "monitoring/MonitoringSupport.hpp"

typedef float busy_secs_t;   // Time busy in seconds
typedef float busy_cores_t;  // Average number of busy cores

class CPU;

/*
 * Simple timer for the cluster utilization statistics.
 * Derived from src/instrumentation/stats/Timer.hpp.
 */
class CPUStatsTimer
{
public:
	typedef struct timespec Time;

private:
	Time _startTime;

public:

	CPUStatsTimer()
	{
		getTime(_startTime);
	}

	static inline void getTime(Time &t)
	{
		int rc = clock_gettime(CLOCK_MONOTONIC, &t);

		if (rc != 0) {
			int error = errno;
			std::cerr << "Error reading time: " << strerror(error) << std::endl;
			exit(1);
		}
	}

	busy_secs_t getElapsed(const Time &lapTime) const
	{
		return (lapTime.tv_sec - _startTime.tv_sec)
			   + (lapTime.tv_nsec - _startTime.tv_nsec) / 1000000000.0;
	}

	busy_secs_t restartTimer(const Time &lapTime)
	{
		busy_secs_t t = getElapsed(lapTime);
		_startTime.tv_sec = lapTime.tv_sec;
		_startTime.tv_nsec = lapTime.tv_nsec;
		return t;
	}

	busy_secs_t restartTimer()
	{
		CPUStatsTimer::Time lapTime;
		CPUStatsTimer::getTime(lapTime);
		return restartTimer(lapTime);
	}
};

// Measure proportion of time that the CPU is busy
class CPUStats {

public:
	// States measured
	enum State {
		NotRunning = 0,
		RunningNoTask,
		RunningInTask,
		AddTask,
		HintAsIdle,
		numStates
	};

private:
	State _state;
	CPUStatsTimer _timer;
	busy_secs_t _times[numStates];
	busy_secs_t _alltimes[numStates];

public:
	CPUStats() : _state(State::NotRunning)
	{
		memset(_times, 0, sizeof(_times));
		memset(_alltimes, 0, sizeof(_alltimes));
	}

	void attributeElapsedTime(float elapsed)
	{
		_times[_state] += elapsed;
	}

	/* CPU starts running again */
	void markCPURunning()
	{
		// During the shutdown process it may already be running
		if (_state != State::RunningNoTask) {
			assert(_state == State::NotRunning || _state == State::HintAsIdle);
			attributeElapsedTime(_timer.restartTimer());
			_state = State::RunningNoTask;
		}
	}

	void taskChangedStatus(monitoring_task_status_t newStatus)
	{
		switch (newStatus) {

			case executing_status:
				assert(_state == State::RunningNoTask || _state == State::AddTask || _state == State::HintAsIdle);
				attributeElapsedTime(_timer.restartTimer());
				_state = State::RunningInTask;
				break;

			case paused_status:
				// If(0) tasks go straight from addtask to paused in enterWaitForIf0
				assert(_state == State::RunningInTask || _state == State::AddTask);
				attributeElapsedTime(_timer.restartTimer());
				_state = State::RunningNoTask;
				break;

			case addtask_status:
				assert(_state == State::RunningInTask);
				attributeElapsedTime(_timer.restartTimer());
				_state = State::AddTask;
				break;

			default:
				break;
		}
	}

	/* CPU goes idle */
	void markCPUIdle()
	{
		assert(_state == State::RunningNoTask || _state == State::HintAsIdle);
		attributeElapsedTime(_timer.restartTimer());
		_state = State::NotRunning;
	}

	/* CPU is running, but CPU policy hints that it is idle
	 * so count it as idle until the next time a task starts
	 * executing
	 */
	void hintAsIdle()
	{
		if (_state != State::HintAsIdle) {
			assert(_state == State::RunningNoTask || _state == State::HintAsIdle);
			attributeElapsedTime(_timer.restartTimer());
			_state = State::HintAsIdle;
		}
	}

	void clearTimeBusy(const CPUStatsTimer::Time &lapTime,
					   busy_secs_t &totalBusyCore,
					   busy_secs_t &usefulBusyCore)
	{
		if (_state != State::NotRunning) {
			attributeElapsedTime(_timer.restartTimer(lapTime));
		}
		totalBusyCore = _times[RunningNoTask] + _times[RunningInTask];
		usefulBusyCore = _times[RunningInTask];
		for(int state=0; state < numStates; state++) {
			_alltimes[state] += _times[state];
			_times[state] = 0;
		}
	}

	void getAllTimes(float times[numStates])
	{
		float tot = 0;
		for(int state=0; state < numStates; state++) {
			_alltimes[state] += _times[state];
			_times[state] = 0;
			tot += _alltimes[state];
		}
		for(int state=0; state < numStates; state++) {
			times[state] = _alltimes[state] / tot;
		}
	}

};

class AveragedStats
{
private:
	double _timePeriodSecs;
	double _movingAverage;
	CPUStatsTimer _timer;
	bool _onStats;
	float _time;
public:
	const bool _totalBusy;

	// Calculate exponential moving average of number of busy cores.
	//
	// This is a linear filter on the #busy cores, whose impulse
	// response drops to 1/e ~ 0.368 after the given length of time, T.
	//
	// Assume a continuous process given by the differential equation:
	//
	//     dY(t)/dt  = alpha . ( X(t) - Y(t) )
	//
	// where X(t) is the number of busy cores being averaged, and
	// Y(t) is the average value. This has a time period t = 1/alpha
	// as the solution for X(t) = 0  [t>0] is Y(t) = e^{-alpha.t}.
	//
	// We are given a value of X and a duration L; assume that X(t)
	// is constant for the duration:
	//
	// So   dY(t)/dt  = alpha . ( X - Y(t) )  
	//
	// Let Z(t) = Y(t) - X; then:
	//
	//      dZ(t)/dt = dY(t)/dt = -alpha . Z(t)
	//
	// So Z(t) = e^(-alpha.t) Z0, where Z0 is the initial value
	//
	//      Y(L) = X + (Y0 - X) e^(-alpha.L)
	//
	AveragedStats(double timePeriodSecs, bool onStats, bool _totalBusy = true);

	~AveragedStats();

	void update(busy_cores_t busyCores, float duration, float timestamp)
	{
		if (timestamp < 1e-5 || duration < 1e-5) {
			// Shouldn't happen but be cautious to avoid exceptions
			return;
		}
		
		double timePeriod = std::min<double>(_timePeriodSecs, timestamp);
		double expVal = exp(-1.0 * duration / timePeriod);
		double newAvg = busyCores + (_movingAverage - busyCores) * expVal;
		_movingAverage = newAvg;
	}

	void update(busy_cores_t busyCores)
	{
		float elapsed = _timer.restartTimer();
		_time += elapsed;
		update(busyCores, elapsed, _time);
	}

	double readBusyCores(void) const
	{
		return _movingAverage;
	}
};

class RuntimeStateMonitor {
private:
	static std::vector<CPUStats *> _CPUStats;
	static CPUStatsTimer _intervalTimer;
	static bool _initialized;
	static busy_cores_t _totalBusyShutdown;
	static busy_cores_t _usefulBusyShutdown;
	static float  _elapsed;
	static std::vector<AveragedStats*> _listeners;
	static int _numCPUs;

public:

	static void initialize();

	// CPU becomes active
	static void cpuBecomesActive(int virtualCPUId)
	{
		assert(virtualCPUId >= 0 && virtualCPUId < _numCPUs);
		_CPUStats[virtualCPUId]->markCPURunning();
	}

	// CPU becomes idle (sleeping)
	static void cpuBecomesIdle(int virtualCPUId)
	{
		assert(virtualCPUId >= 0 && virtualCPUId < _numCPUs);
		_CPUStats[virtualCPUId]->markCPUIdle();
	}

	// CPU enters idle loop
	static void cpuHintAsIdle(int virtualCPUId)
	{
		assert(virtualCPUId >= 0 && virtualCPUId < _numCPUs);
		_CPUStats[virtualCPUId]->hintAsIdle();
	}

	// Update task status
	static void updateTaskStatus(monitoring_task_status_t newStatus, int virtualCPUId)
	{
		assert(virtualCPUId >= 0 && virtualCPUId < _numCPUs);
		_CPUStats[virtualCPUId]->taskChangedStatus(newStatus);
	}


	static void getAllTimes(float times[CPUStats::State::numStates], int virtualCPUId)
	{
		assert(virtualCPUId >= 0 && virtualCPUId < _numCPUs);
		_CPUStats[virtualCPUId]->getAllTimes(times);
	}

	static void taskChangedStatus(monitoring_task_status_t newStatus, int virtualCPUId);

	static inline busy_cores_t readAndClear(float &timestamp, busy_cores_t &usefulBusyCores)
	{
		assert(_initialized);
		// Get time now
		CPUStatsTimer::Time endTime;
		CPUStatsTimer::getTime(endTime);

		// How long has this interval been?
		busy_secs_t time = _intervalTimer.restartTimer(endTime);

		// Count total elapsed time and put in the timestamp
		_elapsed += time;
		timestamp = _elapsed;

		busy_secs_t totalBusy = _totalBusyShutdown;
		busy_secs_t usefulBusy = _usefulBusyShutdown;
		{
			for (CPUStats *cpuStats : _CPUStats) {
				busy_secs_t totalBusyCore, usefulBusyCore;
				cpuStats->clearTimeBusy(endTime, totalBusyCore, usefulBusyCore);
				totalBusy += totalBusyCore;
				usefulBusy += usefulBusyCore;
			}
		}

		_totalBusyShutdown = 0;
		_usefulBusyShutdown = 0;
		busy_cores_t totalBusyCores = (busy_cores_t)(totalBusy / time);
		usefulBusyCores = (busy_cores_t)(usefulBusy / time);

		for (AveragedStats *listener : _listeners) {
			busy_cores_t metric = listener->_totalBusy ? totalBusyCores : usefulBusyCores;
			listener->update(metric, time, _elapsed);
		}

		return totalBusyCores;
	}

	static inline void pushBackListener(AveragedStats *listener)
	{
		_listeners.push_back(listener);
	}

	static inline void removeListener(AveragedStats *listener)
	{
		for(std::vector<AveragedStats *>::iterator it = _listeners.begin();
			it != _listeners.end();
			it++) {
			if (*it == listener) {
				_listeners.erase(it);
				break;
			}
		}
	}

	static void taskChangedStatus(Task *task, monitoring_task_status_t newStatus);

	static void taskCompletedUserCode(Task *task);

	static void enterBlockCurrentTask(Task *task, bool fromUserCode);

	static void exitBlockCurrentTask(Task *task, bool fromUserCode);

	static void enterWaitFor(int virtualCPUId);

	static void exitWaitFor(int virtualCPUId);

	static void displayStatistics(std::stringstream &stream);
};


#endif /* RUNTIMESTATEMONITOR_HPP */
