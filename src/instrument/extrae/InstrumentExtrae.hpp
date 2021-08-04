/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_EXTRAE_HPP
#define INSTRUMENT_EXTRAE_HPP

#include "PreloadedExtraeBouncer.hpp"

#include <nanos6.h>

#include "InstrumentThreadId.hpp"
#include "InstrumentTracingPointTypes.hpp"
#include "support/config/ConfigVariable.hpp"
#include "lowlevel/RWSpinLock.hpp"
#include "lowlevel/SpinLock.hpp"
#include "system/ompss/SpawnFunction.hpp"

#include <atomic>
#include <cstddef>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>


namespace Instrument {
	namespace Extrae {
		extern bool _detailTaskGraph;
		extern bool _detailTaskCount;

		extern bool _extraeInstrumentCluster;
		extern bool _extraeInstrumentDependencies;
		extern bool _extraeInstrumentTaskforChunks;

		extern unsigned int _detailLevel;
		extern bool _traceAsThreads;

		extern bool _initialized;

		class user_fct_t {
			std::string _declaration_source;
			std::string _task_label;
			void *_runFunction;

		public:
			user_fct_t(nanos6_task_info_t *taskInfo)
				: _declaration_source(taskInfo->implementations[0].declaration_source),
				  _task_label(taskInfo->implementations[0].task_label),
				  _runFunction(SpawnFunction::isSpawned(taskInfo)
					  ? taskInfo
					  : (void *)taskInfo->implementations[0].run)
			{
				assert(taskInfo != nullptr);
			}

			// The operator < is required for the insertion in the std::set. The ideal may be to use
			// a tuple. But comparison operator will be removed for tuples in C++-20.
			// This is the code suggested for comparison in:
			// https://en.cppreference.com/w/cpp/utility/tuple/operator_cmp
			bool operator <(const user_fct_t &other) const
			{
				return (_declaration_source < other._declaration_source) ? true
					: (_declaration_source > other._declaration_source) ? false
					: (_task_label < other._task_label) ? true
					: (_task_label > other._task_label) ? false
					: (_runFunction < other._runFunction) ? true
					: (_runFunction > other._runFunction) ? false
					: false;  // This means they are equal.
			}

			void registerFunction() const
			{
				// Remove column
				std::string codeLocation = _declaration_source.substr(
					0,
					_declaration_source.find_last_of(':')
				);
				std::string label = _task_label.empty() ? codeLocation : _task_label;

				// Splice off the line number
				int lineNumber = 0;
				size_t linePosition = codeLocation.find_last_of(':');
				if (linePosition != std::string::npos) {
					std::istringstream iss(codeLocation.substr(linePosition + 1));
					iss >> lineNumber;

					codeLocation.substr(0, linePosition);
				}

				// Use the unique taskInfo address in case it is a spawned task
				ExtraeAPI::register_function_address(
					_runFunction,
					label.c_str(),
					codeLocation.c_str(), lineNumber
				);
			}
		};

		typedef std::set<user_fct_t> user_fct_map_t;

		extern SpinLock _userFunctionMapLock;
		extern user_fct_map_t _userFunctionMap;
	}

	enum {
		THREADS_AND_CPUS_LEVEL = 2
	};

	struct ExtraeTaskInfoCompare {
		inline bool operator()(nanos6_task_info_t *a, nanos6_task_info_t *b) const;
	};

	struct scope_tracing_point_info_t {
		std::string _name;
		std::string _startDescription;
		std::string _endDescription;

		scope_tracing_point_info_t(
			std::string const &name,
			std::string const &startDescription,
			std::string const &endDescription
		) : _name(name),
			_startDescription(startDescription),
			_endDescription(endDescription)
		{
		}
	};

	struct enumerated_tracing_point_info_t {
		std::string _name;
		std::vector<std::string> _valueDescriptions;

		enumerated_tracing_point_info_t(
			std::string const &name,
			std::vector<std::string> const &valueDescriptions
		) : _name(name),
			_valueDescriptions(valueDescriptions)
		{
		}
	};

	extern std::map<tracing_point_type_t, std::string> _delayedNumericTracingPoints;
	extern std::map<tracing_point_type_t, scope_tracing_point_info_t> _delayedScopeTracingPoints;
	extern std::map<tracing_point_type_t, enumerated_tracing_point_info_t> _delayedEnumeratedTracingPoints;


	enum struct EventType {
		// OmpSs common
			RUNTIME_STATE = 9000000,

			TASK_INSTANCE_ID = 9200002,
			RUNNING_FUNCTION_NAME = 9200011,
			RUNNING_TASKFOR_CHUNK = 9200016,
			RUNNING_CODE_LOCATION = 9200021,

			READY_TASKS = 9200022,
			LIVE_TASKS = 9200023,

			PRIORITY = 9200038,

			CPU = 9200042,
			THREAD_NUMA_NODE = 9200064,

		// [9500000:9699999] -- Nanos6-specific
			// 9500XXXX -- Nanos6 Tasking
				NESTING_LEVEL = 9500001,

				INSTANTIATING_FUNCTION_NAME = 9500002,
				INSTANTIATING_CODE_LOCATION = 9500003,

				THREAD = 9500004,

			// 9504XXX -- Reductions
				REDUCTION_STATE = 9504001,

			// 96XXXXX -- Tracing points
				TRACING_POINT_BASE = 9600000,

			// 97XXXXX -- Cluster specific events
				MESSAGE_SEND = 9700000,
				MESSAGE_HANDLE = 9700100,

				NODE_NAMESPACE = 9710000,

				// Note 9800000 to 9800099 reserved for cluster+DLB
				OFFLOADED_TASKS_WAITING = 9800003,
				PENDING_DATA_TRANSFERS = 9800100,
				PENDING_DATA_TRANSFER_BYTES = 9800101,
				PENDING_DATA_TRANSFERS_INCOMING = 9800102,

				// Dependencies system
				DEPENDENCIES_SUBSYSTEM = 9900000,

				DEPENDENCIES_USER_EVENT = 9910000,
	};


	typedef enum {
		NANOS_NO_STATE, NANOS_NOT_RUNNING,
		NANOS_STARTUP, NANOS_SHUTDOWN,
		NANOS_ERROR, NANOS_IDLE,
		NANOS_RUNTIME, NANOS_RUNNING,
		NANOS_SYNCHRONIZATION, NANOS_SCHEDULING,
		NANOS_CREATION, NANOS_THREAD_CREATION,
		NANOS_EVENT_STATE_TYPES
	} nanos6_event_state_t;

	extern char const *_eventStateValueStr[];

	typedef enum {
		NANOS_OUTSIDE_DEPENDENCYSUBSYSTEM,
		NANOS_REGISTERTASKDATAACCESSES,
		NANOS_UNREGISTERTASKDATAACCESSES,
		NANOS_PROPAGATESATISFIABILITY,
		NANOS_RELEASEACCESSREGION,
		NANOS_HANDLEENTERTASKWAIT,
		NANOS_HANDLEEXITTASKWAIT,
		NANOS_UNREGISTERTASKDATAACCESSESCALLBACK,
		NANOS_UNREGISTERTASKDATAACCESSES2,
		NANOS_HANDLECOMPLETEDTASKWAITS,
		NANOS_SETUPTASKWAITWORKFLOW,
		NANOS_RELEASETASKWAITFRAGMENT,
		NANOS_CREATEDATACOPYSTEP_TASK,
		NANOS_CREATEDATACOPYSTEP_TASKWAIT,
		NANOS_TASKDATAACCESSLOCATION,
		NANOS_DEPENDENCY_STATE_TYPES
	} nanos6_dependency_state_t;

	typedef enum {
		NANOS_OUTSIDE_REDUCTION, NANOS_ALLOCATE_REDUCTION_INFO,
		NANOS_RETRIEVE_REDUCTION_STORAGE, NANOS_ALLOCATE_REDUCTION_STORAGE,
		NANOS_INITIALIZE_REDUCTION_STORAGE, NANOS_COMBINE_REDUCTION_STORAGE,
		NANOS_REDUCTION_STATE_TYPES
	} nanos6_reduction_state_t;

	extern char const *_reductionStateValueStr[];
	extern char const *_dependencySubsystemStateValueStr[];

	extern std::atomic<size_t> _nextTaskId;
	extern std::atomic<size_t> _readyTasks;
	extern std::atomic<size_t> _liveTasks;
	extern std::atomic<size_t> _nextTracingPointKey;

	extern RWSpinLock _extraeThreadCountLock;

	extern int _externalThreadCount;

	enum dependency_tag_t {
		instantiation_dependency_tag = 0xffffff00,
		strong_data_dependency_tag,
		weak_data_dependency_tag,
		control_dependency_tag,
		thread_creation_tag
	};


	inline bool ExtraeTaskInfoCompare::operator()(nanos6_task_info_t *a, nanos6_task_info_t *b) const
	{
		std::string labelA(
			a->implementations[0].task_label != nullptr
			? a->implementations[0].task_label
			: a->implementations[0].declaration_source
		);

		std::string labelB(
			b->implementations[0].task_label != nullptr
			? b->implementations[0].task_label
			: b->implementations[0].declaration_source
		);

		if (labelA != labelB) {
			return (labelA < labelB);
		}
		std::string sourceA(a->implementations[0].declaration_source);
		std::string sourceB(b->implementations[0].declaration_source);

		if (sourceA != sourceB) {
			return (sourceA < sourceB);
		}

		void *runA = SpawnFunction::isSpawned(a) ? (void *) a : (void *) a->implementations[0].run;
		void *runB = SpawnFunction::isSpawned(b) ? (void *) b : (void *) b->implementations[0].run;

		return (runA < runB);
	}

	unsigned int extrae_nanos6_get_num_threads();
	unsigned int extrae_nanos6_get_num_cpus_and_external_threads();
}

#endif
