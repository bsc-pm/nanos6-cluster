/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include <cassert>
#include <cstdlib>
#include <dlfcn.h>
#include <mutex>
#include <sstream>
#include <algorithm>

#include <nanos6/debug.h>

#include "InstrumentExtrae.hpp"
#include "InstrumentInitAndShutdown.hpp"
#include "InstrumentThreadId.hpp"
#include "InstrumentThreadLocalData.hpp"
#include "InstrumentCluster.hpp"
#include "instrument/api/InstrumentInitAndShutdown.hpp"
#include "instrument/generic_ids/GenericIds.hpp"
#include "instrument/support/InstrumentThreadLocalDataSupport.hpp"
#include "system/RuntimeInfo.hpp"
#include "system/ompss/SpawnFunction.hpp"
#include "lowlevel/TokenizedEnvironmentVariable.hpp"
#include "ClusterManager.hpp"
#include "InstrumentDependencySubsystemEntryPoints.hpp"

using namespace Instrument::Extrae;

namespace Instrument {

	static unsigned int numtasks;
	static unsigned int task_id;

	static unsigned int extrae_nanos6_get_numtasks()
	{
		return numtasks;
	}

	static unsigned int extrae_nanos6_get_task_id()
	{
		return task_id;
	}

	static unsigned int extrae_nanos6_get_thread_id()
	{
		ThreadLocalData &threadLocal = getThreadLocalData();
		if (threadLocal._currentThreadId == thread_id_t()) {
			ExternalThreadLocalData &externalThreadLocalData = getExternalThreadLocalData();
			return externalThreadLocalData._currentThreadId;
		} else {
			return threadLocal._currentThreadId;
		}
	}

	static unsigned int extrae_nanos6_get_virtual_cpu_or_external_thread_id()
	{
		ThreadLocalData &threadLocal = getThreadLocalData();
		if (threadLocal._currentThreadId == thread_id_t()) {
			ExternalThreadLocalData &externalThreadLocalData = getExternalThreadLocalData();

			// We use "total_num_cpus" since, when DLB is enabled, any CPU in the
			// system might emit events
			return nanos6_get_total_num_cpus() + externalThreadLocalData._currentThreadId;
		} else {
			return nanos6_get_current_virtual_cpu();
		}
	}


	void initialize()
	{
		ExtraeSymbolResolverBase::initialize();
		RuntimeInfo::addEntry("instrumentation", "Instrumentation", "extrae");

		ConfigVariableVector<std::string> extraeAreas("instrument.extrae.areas");

		for (auto area : extraeAreas) {
			std::transform(area.begin(), area.end(), area.begin(), ::tolower);
			if (area == "all") {
				_detailTaskGraph =  true;
				_detailTaskCount =  true;
				_extraeInstrumentDependencies = true;
				_extraeInstrumentCluster = true;
				_extraeInstrumentTaskforChunks = true;

			} else if (area == "taskgraph") {
				_detailTaskGraph =  true;
			} else if (area == "taskcount") {
				_detailTaskCount =  true;
			} else if (area == "dependencysystem") {
				_extraeInstrumentDependencies = true;
			} else if (area == "cluster") {
				_extraeInstrumentCluster = true;
			} else if (area == "taskforchunks") {
				_extraeInstrumentTaskforChunks = true;

			} else if (area == "!taskgraph") {
				_detailTaskGraph =  false;
			} else if (area == "!taskcount") {
				_detailTaskCount =  false;
			} else if (area == "!dependencysystem") {
				_extraeInstrumentDependencies = false;
			} else if (area == "!cluster") {
				_extraeInstrumentCluster = false;
			} else if (area == "!taskforchunks") {
				_extraeInstrumentTaskforChunks = false;


			} else {
				std::cerr << "Warning: ignoring unknown '" << area << "' extrae instrumentation" << std::endl;
			}
		}

		const ConfigVariable<unsigned int> detailLevel("instrument.extrae.detail_level");
		_detailLevel = detailLevel.getValue();

		const ConfigVariable<bool> traceAsThreads("instrument.extrae.as_threads");
		_traceAsThreads = traceAsThreads.getValue();


		// This is a workaround to avoid an extrae segfault
		if ((getenv("EXTRAE_ON") == nullptr) && (getenv("EXTRAE_CONFIG_FILE") == nullptr)) {
			setenv("EXTRAE_ON", "1", 0);
		}

		{
			__attribute__((unused)) ThreadLocalData &threadLocal = getThreadLocalData();
			__attribute__((unused)) ExternalThreadLocalData &externalThreadLocal = getExternalThreadLocalData();
		}

		RuntimeInfo::addEntry("instrumentation", "Instrumentation", "extrae");

		if (getenv("EXTRAE_CONFIG_FILE") != nullptr) {
			RuntimeInfo::addEntry(
				"extrae_config_file",
				"Extrae Configuration File",
				getenv("EXTRAE_CONFIG_FILE")
			);
		}

		// Set up thread information callbacks before initiating library, so
		// that the Extrae backend is initialized with the right number of tasks
		// for clusters.
		// Thread information callbacks
		if (ClusterManager::clusterRequested()) {
			// Note: Extrae may call these functions after the ClusterManager
			// has been shutdown, so take a copy of these values now.
			numtasks = ClusterManager::getNumExternalRanks();
			task_id = ClusterManager::getInstrumentationRank();
			ExtraeAPI::set_taskid_function(extrae_nanos6_get_task_id);
			ExtraeAPI::set_numtasks_function(extrae_nanos6_get_numtasks);
		}

		if (_traceAsThreads) {
			ExtraeAPI::set_threadid_function(extrae_nanos6_get_thread_id);
			ExtraeAPI::set_numthreads_function(extrae_nanos6_get_num_threads);
			ExtraeAPI::change_num_threads(extrae_nanos6_get_num_threads());
			RuntimeInfo::addEntry("extrae_tracing_target", "Extrae Tracing Target", "thread");
		} else {
			ExtraeAPI::set_threadid_function(extrae_nanos6_get_virtual_cpu_or_external_thread_id);
			ExtraeAPI::set_numthreads_function(extrae_nanos6_get_num_cpus_and_external_threads);
			ExtraeAPI::change_num_threads(extrae_nanos6_get_num_cpus_and_external_threads());
			RuntimeInfo::addEntry("extrae_tracing_target", "Extrae Tracing Target", "cpu");
		}

		if (ExtraeAPI::is_initialized() == EXTRAE_NOT_INITIALIZED) {
			// Initialize extrae library. It may be already initialized by the MPI_Init. So we
			// condition here to avoid the warning.
			ExtraeAPI::init();
		}

		ExtraeAPI::register_codelocation_type(
			(extrae_type_t) EventType::RUNNING_FUNCTION_NAME,
			(extrae_type_t) EventType::RUNNING_CODE_LOCATION,
			(char *) "Running Name", (char *) "Running Location"
		);
		ExtraeAPI::register_codelocation_type(
			(extrae_type_t) EventType::INSTANTIATING_FUNCTION_NAME,
			(extrae_type_t) EventType::INSTANTIATING_CODE_LOCATION,
			(char *) "Instantiating Name", (char *) "Instantiating Location"
		);
		ExtraeAPI::define_event_type(
			(extrae_type_t) EventType::TASK_INSTANCE_ID, "Task instance", 0, nullptr, nullptr
		);
		ExtraeAPI::define_event_type(
			(extrae_type_t) EventType::NESTING_LEVEL, "Task nesting level", 0, nullptr, nullptr
		);

		ExtraeAPI::define_event_type(
			(extrae_type_t) EventType::READY_TASKS, "Number of ready tasks", 0, nullptr, nullptr
		);
		ExtraeAPI::define_event_type(
			(extrae_type_t) EventType::LIVE_TASKS, "Number of live tasks", 0, nullptr, nullptr
		);

		ExtraeAPI::define_event_type(
			(extrae_type_t) EventType::PRIORITY, "Task priority", 0, nullptr, nullptr
		);

		if (_detailLevel >= (int) THREADS_AND_CPUS_LEVEL) {
			if (_traceAsThreads) {

				ExtraeAPI::define_event_type(
					(extrae_type_t) EventType::CPU, "CPU", 0, nullptr, nullptr
				);

				ExtraeAPI::define_event_type(
					(extrae_type_t) EventType::THREAD_NUMA_NODE, "NUMA Node", 0, nullptr, nullptr
				);
			} else {
				ExtraeAPI::define_event_type(
					(extrae_type_t) EventType::THREAD, "Thread", 0, nullptr, nullptr
				);
			}
		}

		// This is to define the cluster event names and messages
		// TODO: Maybe this could be conditioned in cluster mode only.
		defineClusterExtraeEvents();

		// Register runtime states
		{
			extrae_value_t values[NANOS_EVENT_STATE_TYPES];
			unsigned int i;

			for (i = 0; i < NANOS_EVENT_STATE_TYPES; i++) {
				values[i] = i;
			}

			ExtraeAPI::define_event_type(
				(extrae_type_t) EventType::RUNTIME_STATE,
				"Runtime state",
				NANOS_EVENT_STATE_TYPES,
				values,
				_eventStateValueStr
			);
		}

		// Register reduction states
		{
			extrae_value_t values[NANOS_REDUCTION_STATE_TYPES];
			unsigned int i;

			for (i = 0; i < NANOS_REDUCTION_STATE_TYPES; i++) {
				values[i] = i;
			}

			ExtraeAPI::define_event_type(
				(extrae_type_t) EventType::REDUCTION_STATE,
				"Reduction state",
				NANOS_REDUCTION_STATE_TYPES,
				values,
				_reductionStateValueStr
			);
		}

		// Register dependency subsystem states
		{
			extrae_value_t values[NANOS_DEPENDENCY_STATE_TYPES];
			unsigned int i;

			for (i = 0; i < NANOS_DEPENDENCY_STATE_TYPES; i++) {
				values[i] = i;
			}

			ExtraeAPI::define_event_type(
				(extrae_type_t) EventType::DEPENDENCIES_SUBSYSTEM,
				"Dependency subsystem state",
				NANOS_DEPENDENCY_STATE_TYPES,
				values,
				_dependencySubsystemStateValueStr
			);

			ExtraeAPI::define_event_type(
				(extrae_type_t) EventType::DEPENDENCIES_USER_EVENT,
				"Dependency user event",
				0,
				nullptr,
				nullptr
			);

		}


		// Force an event that allows to detect the trace as an OmpSs trace
		{
			extrae_combined_events_t ce;

			ce.HardwareCounters = 0;
			ce.Callers = 0;
			ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
			ce.nEvents = 1;
			ce.nCommunications = 0;

			ce.Types  = (extrae_type_t *)  alloca(ce.nEvents * sizeof(extrae_type_t) );
			ce.Values = (extrae_value_t *) alloca(ce.nEvents * sizeof(extrae_value_t));

			ce.Types[0] = 9200001;
			ce.Values[0] = 0;

			ExtraeAPI::emit_CombinedEvents(&ce);
		}

		_initialized = true;

		// Register any tracing point that arrived too early
		for (auto &p : _delayedNumericTracingPoints) {
			ExtraeAPI::define_event_type(
				(extrae_type_t) EventType::TRACING_POINT_BASE + p.first._type,
				p.second.c_str(),
				0, nullptr, nullptr
			);
		}
		for (auto &p : _delayedScopeTracingPoints) {
			extrae_value_t values[2] = {0, 1};
			char const *valueDescriptions[2] = {
				p.second._startDescription.c_str(),
				p.second._endDescription.c_str()
			};

			ExtraeAPI::define_event_type(
				(extrae_type_t) EventType::TRACING_POINT_BASE + p.first._type,
				p.second._name.c_str(),
				2,
				values,
				valueDescriptions
			);
		}
		for (auto &p : _delayedEnumeratedTracingPoints) {
			extrae_value_t values[p.second._valueDescriptions.size()];
			char const *extraeValueDescriptions[p.second._valueDescriptions.size()];

			for (size_t i = 0; i < p.second._valueDescriptions.size(); i++) {
				values[i] = i;
				extraeValueDescriptions[i] = p.second._valueDescriptions[i].c_str();
			}

			ExtraeAPI::define_event_type(
				(extrae_type_t) EventType::TRACING_POINT_BASE + p.first._type,
				p.second._name.c_str(),
				p.second._valueDescriptions.size(),
				values,
				extraeValueDescriptions
			);
		}

		{
			std::stringstream oss;
			unsigned extraeMajor, extraeMinor, extraeRevision;

			ExtraeAPI::get_version(&extraeMajor, &extraeMinor, &extraeRevision);
			oss << extraeMajor << "." << extraeMinor << "." << extraeRevision;
			RuntimeInfo::addEntry("extrae_version", "Extrae Version", oss.str());
			RuntimeInfo::addEntry(
				"extrae_shared_object",
				"Extrae Shared Object",
				ExtraeSymbolResolverBase::getSharedObjectPath()
			);
		}
	}


	void shutdown()
	{
		{
			std::lock_guard<SpinLock> guard(_userFunctionMapLock);
			for (const Extrae::user_fct_t &taskInfo : _userFunctionMap) {
				taskInfo.registerFunction();
			}
		}

		// Finalize extrae library
		ExtraeAPI::fini();
	}
}

