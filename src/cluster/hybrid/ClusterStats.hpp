/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_STATS_HPP
#define CLUSTER_STATS_HPP

#include <algorithm>
#include <math.h>
#include <time.h>
#include <unordered_map>
#include "ClusterManager.hpp"
#include "lowlevel/SpinLock.hpp"

typedef float busy_secs_t;   // Time busy in seconds
typedef float busy_cores_t;  // Average number of busy cores

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
	bool _isRunning;

public:

	CPUStatsTimer() : _isRunning(false)
	{
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

	inline void startTimer()
	{
		assert(!_isRunning);
		getTime(_startTime);
		_isRunning = true;
	}

	busy_secs_t getElapsed(const Time &lapTime) const
	{
		assert(_isRunning);

		return (lapTime.tv_sec - _startTime.tv_sec)
			   + (lapTime.tv_nsec - _startTime.tv_nsec) / 1000000000.0;
	}

	busy_secs_t stopTimer()
	{
		Time endTime;
		getTime(endTime);

		busy_secs_t t = getElapsed(endTime);

		_isRunning = false;
		return t;
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
class ClusterCPUStats {

	enum State {
		NotRunning = 0,
		RunningNoTask,
		RunningInTask,
		HintAsIdle
	};

private:
	State _state;
	// bool _isRunning;   // thread is running on the CPU
	// bool _hintAsIdle;  // thread is running, but hinted as idle
	// bool _isBusy;      // thread is executing a task (but may be blocked)
	CPUStatsTimer _timer;
	busy_secs_t _totalBusyTime;
	busy_secs_t _usefulBusyTime;
public:
	ClusterCPUStats() : _state(State::NotRunning), _totalBusyTime(0), _usefulBusyTime(0)
	{
	}

	void attributeElapsedTime(float elapsed)
	{
		switch(_state) {
			case NotRunning:
			case HintAsIdle:
				/* do nothing, don't count this time */
				break;
			case RunningNoTask:
				/* Count in total busy time (which includes overhead and waiting for data transfers) */
				_totalBusyTime += elapsed;
				break;
			case RunningInTask:
				/* Count in both total and useful busy time */
				_totalBusyTime += elapsed;
				_usefulBusyTime += elapsed;
				break;
		}
	}

	/* CPU starts running again */
	void markCPURunning()
	{
		assert(_state == State::NotRunning);
		_timer.startTimer();
		_state = State::RunningNoTask;
	}

	void startTask()
	{
		assert(_state == State::RunningNoTask || _state == State::HintAsIdle);
		attributeElapsedTime(_timer.restartTimer());
		_state = State::RunningInTask;
	}

	void endTask()
	{
		assert(_state == State::RunningInTask);
		attributeElapsedTime(_timer.restartTimer());
		_state = State::RunningNoTask;
	}

	/* CPU goes idle */
	void markCPUIdle()
	{
		assert(_state == State::RunningNoTask || _state == State::HintAsIdle);
		attributeElapsedTime(_timer.stopTimer());
		_state = State::NotRunning;
	}

	/* CPU is running, but CPU policy hints that it is idle
	 * so count it as idle until the next time a task starts
	 * executing
	 */
	void hintAsIdle()
	{
		assert(_state == State::RunningNoTask || _state == State::HintAsIdle);
		attributeElapsedTime(_timer.restartTimer());
		_state = State::HintAsIdle;
	}

	void clearTimeBusy(const CPUStatsTimer::Time &lapTime,
					   busy_secs_t &totalBusyCore,
					   busy_secs_t &usefulBusyCore)
	{
		if (_state != State::NotRunning) {
			attributeElapsedTime(_timer.restartTimer(lapTime));
		}
		totalBusyCore = _totalBusyTime;
		usefulBusyCore = _usefulBusyTime;
		_totalBusyTime = 0;
		_usefulBusyTime = 0;
	}
};

class ClusterAveragedStats
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
	ClusterAveragedStats(double timePeriodSecs, bool onStats, bool _totalBusy = true);

	~ClusterAveragedStats();

	void update(busy_cores_t busyCores, float duration, float timestamp)
	{
		if (timestamp < 1e-5 || duration < 1e-5) {
			// Shouldn't happen but be cautious to avoid exceptions
			return;
		}
		
		double timePeriod = std::min<double>(_timePeriodSecs, timestamp);
		double expVal = exp(-1.0 * duration / timePeriod);
		double newAvg = busyCores + (_movingAverage - busyCores) * expVal;
#if 0
		std::cout << "Extrank " << ClusterManager::getExternalRank() << ": avg=" << newAvg
					<< " after " << busyCores << " for " << duration << "\n";
#endif
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

class ClusterStats {
private:
	static std::unordered_map<void *, ClusterCPUStats *> _CPUStats;
	static CPUStatsTimer _intervalTimer;
	static SpinLock _lock;
	static bool _initialized;
	static busy_cores_t _totalBusyShutdown;
	static busy_cores_t _usefulBusyShutdown;
	static float  _elapsed;
	static std::vector<ClusterAveragedStats*> _listeners;

public:

	static void initialize()
	{
		// std::cout << ClusterManager::getCurrentClusterNode()->getCommIndex() << " ClusterStats::initialize\n";
		assert(!_initialized);

		_intervalTimer.startTimer();
		_initialized = true;
		_totalBusyShutdown = 0;
		_usefulBusyShutdown = 0;
		_elapsed = 0;

		// Some timers start before initialize. Rather than making this an
		// error, and adding another dependency to the start up process, simply
		// clear any busy time already accumulated.
		CPUStatsTimer::Time endTime;
		CPUStatsTimer::getTime(endTime);
		std::lock_guard<SpinLock> guard(_lock);
		for (auto &it : _CPUStats) {
				ClusterCPUStats *cpuStats = it.second;
				busy_secs_t totalBusyCore, usefulBusyCore; // ignore return values
				cpuStats->clearTimeBusy(endTime, totalBusyCore, usefulBusyCore);
		}
	}

	static void createdThread(void *thread)
	{
		std::lock_guard<SpinLock> guard(_lock);
		// std::cout << ClusterManager::getCurrentClusterNode()->getCommIndex() << " threadCreated " << thread << "\n";
		assert(_CPUStats.count(thread) == 0);
		_CPUStats[thread] = new ClusterCPUStats();
	}

	static void threadWillShutdown(void *thread)
	{
		std::lock_guard<SpinLock> guard(_lock);
		// std::cout << ClusterManager::getCurrentClusterNode()->getCommIndex() << " threadShutdown " << thread << "\n";
		assert(_CPUStats.count(thread) == 1);
		ClusterCPUStats *cpuStats = _CPUStats[thread];

		CPUStatsTimer::Time endTime;
		CPUStatsTimer::getTime(endTime);
		float totalBusy, usefulBusy;
		cpuStats->clearTimeBusy(endTime, totalBusy, usefulBusy);
		_totalBusyShutdown += totalBusy;
		_usefulBusyShutdown += usefulBusy;

		delete cpuStats;
		_CPUStats.erase(thread);
	}

	static void threadWillSuspend(void *thread)
	{
		std::lock_guard<SpinLock> guard(_lock);
		// std::cout << ClusterManager::getCurrentClusterNode()->getCommIndex() << " threadWillSuspend " << thread << "\n";
		assert(_CPUStats.count(thread) == 1);
		_CPUStats[thread]->markCPUIdle();
	}

	static void threadHasResumed(void *thread)
	{
		std::lock_guard<SpinLock> guard(_lock);
		assert(_CPUStats.count(thread) == 1);
		_CPUStats[thread]->markCPURunning();
	}

	static inline void startTask(void *thread)
	{
		std::lock_guard<SpinLock> guard(_lock);
		assert(_CPUStats.count(thread) == 1);
		_CPUStats[thread]->startTask();
	}

	static inline void endTask(void *thread)
	{
		std::lock_guard<SpinLock> guard(_lock);
		assert(_CPUStats.count(thread) == 1);
		_CPUStats[thread]->endTask();
	}

	static inline void leaveTask(void *thread)
	{
		endTask(thread);
	}

	static inline void returnToTask(void *thread)
	{
		startTask(thread);
	}

	static inline void hintAsIdle(void *thread)
	{
		std::lock_guard<SpinLock> guard(_lock);
		assert(_CPUStats.count(thread) == 1);
		_CPUStats[thread]->hintAsIdle();
	}

	static inline busy_cores_t readAndClearCoresBusy(float &timestamp, busy_cores_t &usefulBusyCores)
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
			std::lock_guard<SpinLock> guard(_lock);
			for (auto &it : _CPUStats) {
				ClusterCPUStats *cpuStats = it.second;
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

		for (ClusterAveragedStats *listener : _listeners) {
			busy_cores_t metric = listener->_totalBusy ? totalBusyCores : usefulBusyCores;
			listener->update(metric, time, _elapsed);
		}

		return totalBusyCores;
	}

	static inline void pushBackListener(ClusterAveragedStats *listener)
	{
		std::lock_guard<SpinLock> guard(_lock);
		_listeners.push_back(listener);
	}

	static inline void removeListener(ClusterAveragedStats *listener)
	{
		std::lock_guard<SpinLock> guard(_lock);
		for(std::vector<ClusterAveragedStats *>::iterator it = _listeners.begin();
			it != _listeners.end();
			it++) {
			if (*it == listener) {
				_listeners.erase(it);
				break;
			}
		}
	}
};


#endif /* CLUSTER_STATS_HPP */


