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
		Extrae::TaskInfo *_taskInfo;

		task_id_t(Extrae::TaskInfo *taskInfo = nullptr) : _taskInfo(taskInfo)
		{
		}

		task_id_t(task_id_t const &other) : _taskInfo(other._taskInfo)
		{
		}

		task_id_t& operator=(const task_id_t& other)
		{
			_taskInfo = other._taskInfo;
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
			nanos6_task_info_t *_taskInfo;
			size_t _taskId;
			int _nestingLevel;
			long _priority;
			Instrument::task_id_t _parent;

			std::atomic<bool> _inTaskwait;

			SpinLock _lock;
			std::set<predecessor_entry_t> _predecessors;

			TaskInfo()
				: _taskInfo(nullptr), _taskId(~0UL), _nestingLevel(-1), _priority(0), _parent(),
				  _inTaskwait(false), _lock(), _predecessors()
			{
			}

			TaskInfo(
				nanos6_task_info_t *taskInfo,
				int nestingLevel,
				const Instrument::task_id_t &parent
			) : _taskInfo(taskInfo), _nestingLevel(nestingLevel), _priority(0), _parent(parent),
				_inTaskwait(false), _lock(), _predecessors()
			{
				_taskId = _nextTaskId++;
			}
		};
	}


	inline task_id_t::operator long() const
	{
		if (_taskInfo != nullptr) {
			return (long) _taskInfo->_taskId;
		}
		return 0;
	}
}


#endif // INSTRUMENT_EXTRAE_TASK_ID_HPP
