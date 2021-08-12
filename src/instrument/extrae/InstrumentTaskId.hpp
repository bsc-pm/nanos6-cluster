/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_EXTRAE_TASK_ID_HPP
#define INSTRUMENT_EXTRAE_TASK_ID_HPP


#include <nanos6.h>

#include <lowlevel/SpinLock.hpp>

#include "InstrumentExtrae.hpp"

#include <atomic>
#include <set>


namespace Instrument {

	namespace Extrae {
		struct TaskInfo;
	}

	struct task_id_t {
	private:
		void inc();
		void dec();
	public:
		Extrae::TaskInfo *_taskInfo;

		task_id_t(Extrae::TaskInfo *taskInfo = nullptr) : _taskInfo(taskInfo)
		{
			inc();
		}

		task_id_t(task_id_t const &other) : _taskInfo(other._taskInfo)
		{
			inc();
		}

		~task_id_t()
		{
			dec();
		}

		task_id_t& operator=(const task_id_t& other)
		{
			if (_taskInfo != other._taskInfo) {
				task_id_t tmp = *this;        // trick to avoid early collection.
				dec();
				_taskInfo = other._taskInfo;
				inc();
			}
			return *this;
		}

		bool operator==(task_id_t const &other) const
		{
			return (_taskInfo == other._taskInfo);
		}

		bool operator!=(task_id_t const &other) const
		{
			return (*this == other) == false;
		}

		bool operator<(task_id_t const &other) const
		{
			return (_taskInfo < other._taskInfo);
		}

		operator long() const;
	};

	namespace Extrae {
		typedef std::pair<size_t, dependency_tag_t> predecessor_entry_t; // Task and strength

		struct TaskInfo {
		private:
			std::atomic<int> _refCount;
			friend class Instrument::task_id_t;

		public:
			TaskInfo &operator=(const TaskInfo &) = delete;

			nanos6_task_info_t *_taskInfo;
			size_t _taskId;
			int _nestingLevel;
			long _priority;
			Instrument::task_id_t _parent;

			std::atomic<bool> _inTaskwait;

			SpinLock _lock;
			std::set<predecessor_entry_t> _predecessors;

			TaskInfo()
				: _refCount(0), _taskInfo(nullptr), _taskId(~0UL),
				  _nestingLevel(-1), _priority(0),
				  _parent(), _inTaskwait(false), _lock(), _predecessors()
			{
			}

			TaskInfo(
				nanos6_task_info_t *taskInfo,
				int nestingLevel,
				const Instrument::task_id_t &parent
			) : _refCount(0), _taskInfo(taskInfo), _taskId(_nextTaskId++),
				_nestingLevel(nestingLevel), _priority(0),
				_parent(parent), _inTaskwait(false), _lock(), _predecessors()
			{
			}

			TaskInfo(const TaskInfo&) = delete;
		};
	}


	inline task_id_t::operator long() const
	{
		if (_taskInfo != nullptr) {
			return (long) _taskInfo->_taskId;
		}
		return 0;
	}

	inline void task_id_t::inc()
	{
		if (_taskInfo != nullptr) {
			const __attribute__((unused)) int var = _taskInfo->_refCount.fetch_add(1);
			assert(var >= 0);
		}
	}

	inline void task_id_t::dec()
	{
		if (_taskInfo != nullptr) {
			const int var = _taskInfo->_refCount.fetch_sub(1) - 1;
			assert(var >= 0);
			if (var == 0) {
				delete _taskInfo;
				_taskInfo = nullptr;
			}
		}
	}


}


#endif // INSTRUMENT_EXTRAE_TASK_ID_HPP
