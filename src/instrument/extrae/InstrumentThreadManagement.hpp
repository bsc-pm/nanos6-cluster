/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_EXTRAE_THREAD_MANAGEMENT_HPP
#define INSTRUMENT_EXTRAE_THREAD_MANAGEMENT_HPP


#include "InstrumentExtrae.hpp"
#include "instrument/api/InstrumentThreadManagement.hpp"
#include "../generic_ids/GenericIds.hpp"
#include "../support/InstrumentThreadLocalDataSupport.hpp"


namespace Instrument {
	inline void enterThreadCreation(/* OUT */ thread_id_t &threadId, __attribute__((unused)) compute_place_id_t const &computePlaceId)
	{
		threadId = GenericIds::getNewThreadId();

		extrae_combined_events_t ce;

		ce.HardwareCounters = 1;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;

		if (Extrae::_detailLevel > 0) {
			ce.nCommunications++;
		}

		ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
		ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

		if (ce.nCommunications > 0) {
			ce.Communications = (extrae_user_communication_t *) alloca(sizeof(extrae_user_communication_t) * ce.nCommunications);
		}

		ce.Types[0] = (extrae_type_t) EventType::RUNTIME_STATE;
		ce.Values[0] = (extrae_value_t) NANOS_THREAD_CREATION;

		if (Extrae::_detailLevel > 0) {
			ce.Communications[0].type = EXTRAE_USER_SEND;
			ce.Communications[0].tag = (extrae_comm_tag_t) thread_creation_tag;
			ce.Communications[0].size = 0;
			ce.Communications[0].partner = EXTRAE_COMM_PARTNER_MYSELF;
			ce.Communications[0].id = threadId;
		}

		Extrae::emit_CombinedEvents ( &ce );
	}

	inline void recordRuntimeState(extrae_value_t value) 
	{
		extrae_combined_events_t ce;

		ce.HardwareCounters = 1;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;

		ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
		ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

		ce.Types[0] = (extrae_type_t) EventType::RUNTIME_STATE;
		ce.Values[0] = value;

		Extrae::emit_CombinedEvents ( &ce );
	}

	inline void exitThreadCreation(__attribute__((unused)) thread_id_t threadId)
	{
		recordRuntimeState( (extrae_value_t) NANOS_RUNTIME );
	}

	inline void enterBusyWait()
	{
		recordRuntimeState( (extrae_value_t) NANOS_IDLE );
	}

	inline void exitBusyWait()
	{
		recordRuntimeState( (extrae_value_t) NANOS_RUNTIME );
	}

	inline void createdThread(thread_id_t threadId, __attribute__((unused)) compute_place_id_t const &computePlaceId)
	{
		ThreadLocalData &threadLocal = getThreadLocalData();

		threadLocal._nestingLevels.push_back(0);

		threadLocal._currentThreadId = threadId;

		if (Extrae::_traceAsThreads) {
			Extrae::_extraeThreadCountLock.writeLock();
			Extrae::_lockMPI.lock();
			ExtraeAPI::change_num_threads(extrae_nanos6_get_num_threads());
			Extrae::_lockMPI.unlock();
			Extrae::_extraeThreadCountLock.writeUnlock();
		}

		extrae_combined_events_t ce;

		ce.HardwareCounters = 1;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;

		if (Extrae::_detailLevel >= (int) THREADS_AND_CPUS_LEVEL) {
			if (Extrae::_traceAsThreads) {
				ce.nEvents += 2; // CPU, THREAD_NUMA_NODE
			} else {
				ce.nEvents++; // THREAD
			}
		}

		if (Extrae::_detailLevel > 0) {
			ce.nCommunications++;
		}

		ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
		ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

		if (ce.nCommunications > 0) {
			ce.Communications = (extrae_user_communication_t *) alloca(sizeof(extrae_user_communication_t) * ce.nCommunications);
		}

		ce.Types[0] = (extrae_type_t) EventType::RUNTIME_STATE;
		ce.Values[0] = (extrae_value_t) NANOS_STARTUP;

		if (Extrae::_detailLevel >= (int) THREADS_AND_CPUS_LEVEL) {
			if (Extrae::_traceAsThreads) {
				ce.Types[1] = (extrae_type_t) EventType::CPU;
				ce.Values[1] = (extrae_value_t) (computePlaceId._id + 1);
				ce.Types[2] = (extrae_type_t) EventType::THREAD_NUMA_NODE;
				ce.Values[2] = (extrae_value_t) (computePlaceId._NUMANode + 1);
			} else {
				ce.Types[1] = (extrae_type_t) EventType::THREAD;
				ce.Values[1] = (extrae_value_t) (threadId + 1);
			}
		}

		if (Extrae::_detailLevel > 0) {
			ce.Communications[0].type = EXTRAE_USER_RECV;
			ce.Communications[0].tag = (extrae_comm_tag_t) thread_creation_tag;
			ce.Communications[0].size = 0;
			ce.Communications[0].partner = EXTRAE_COMM_PARTNER_MYSELF;
			ce.Communications[0].id = threadId;
		}

		Extrae::emit_CombinedEvents ( &ce );
	}

	inline void precreatedExternalThread(/* OUT */ external_thread_id_t &threadId)
	{
		ExternalThreadLocalData &threadLocal = getExternalThreadLocalData();

		// Force the sentinel worker TLS to be initialized
		{
			__attribute__((unused)) ThreadLocalData &sentinelThreadLocal = getThreadLocalData();
		}

		if (Extrae::_traceAsThreads) {
			// Same thread counter as regular worker threads
			threadId = GenericIds::getCommonPoolNewExternalThreadId();
		} else {
			// Conter separated from worker threads
			threadId = GenericIds::getNewExternalThreadId();
		}

		threadLocal._currentThreadId = threadId;
	}

	template<typename... TS>
	void createdExternalThread(__attribute__((unused)) external_thread_id_t &threadId, __attribute__((unused)) TS... nameComponents)
	{
		extrae_combined_events_t ce;

		ce.HardwareCounters = 1;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;

		ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
		ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

		ce.Types[0] = (extrae_type_t) EventType::RUNTIME_STATE;
		ce.Values[0] = (extrae_value_t) NANOS_IDLE;

		Extrae::_extraeThreadCountLock.writeLock();
		Extrae::_lockMPI.lock();
		if (Extrae::_traceAsThreads) {
			ExtraeAPI::change_num_threads(extrae_nanos6_get_num_threads());
		} else {
			ExtraeAPI::change_num_threads(extrae_nanos6_get_num_cpus_and_external_threads());
		}
		Extrae::_lockMPI.unlock();
		Extrae::_extraeThreadCountLock.writeUnlock();

		Extrae::emit_CombinedEvents ( &ce );
	}

	inline void threadSynchronizationCompleted(
		__attribute((unused)) thread_id_t threadId
	) {
	}

	inline void threadWillSuspend(
		__attribute__((unused)) thread_id_t threadId,
		__attribute__((unused)) compute_place_id_t cpu,
		__attribute__((unused)) bool afterSynchronization
	) {
		if (Extrae::_traceAsThreads) {
			extrae_combined_events_t ce;

			ce.HardwareCounters = 0;
			ce.Callers = 0;
			ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
			ce.nEvents = 1;
			ce.nCommunications = 0;

			if (Extrae::_detailLevel >= (int) THREADS_AND_CPUS_LEVEL) {
				if (Extrae::_traceAsThreads) {
					ce.nEvents += 2; // CPU, THREAD_NUMA_NODE
				} else {
					ce.nEvents++; // THREAD
				}
			}

			ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
			ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

			ce.Types[0] = (extrae_type_t) EventType::RUNTIME_STATE;
			ce.Values[0] = (extrae_value_t) NANOS_NOT_RUNNING;

			if (Extrae::_detailLevel >= (int) THREADS_AND_CPUS_LEVEL) {
				if (Extrae::_traceAsThreads) {
					ce.Types[1] = (extrae_type_t) EventType::CPU;
					ce.Values[1] = (extrae_value_t) 0;
					ce.Types[2] = (extrae_type_t) EventType::THREAD_NUMA_NODE;
					ce.Values[2] = (extrae_value_t) 0;
				} else {
					ce.Types[1] = (extrae_type_t) EventType::THREAD;
					ce.Values[1] = (extrae_value_t) 0;
				}
			}

			Extrae::emit_CombinedEvents ( &ce );
		}
	}

	inline void threadHasResumed(
		__attribute__((unused)) thread_id_t threadId,
		__attribute__((unused)) compute_place_id_t cpu,
		__attribute__((unused)) bool afterSynchronization
	) {
		if (Extrae::_traceAsThreads) {
			extrae_combined_events_t ce;

			ce.HardwareCounters = 0;
			ce.Callers = 0;
			ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
			ce.nEvents = 1;
			ce.nCommunications = 0;

			if (Extrae::_detailLevel >= (int) THREADS_AND_CPUS_LEVEL) {
				if (Extrae::_traceAsThreads) {
					ce.nEvents += 2; // CPU, THREAD_NUMA_NODE
				} else {
					ce.nEvents++; // THREAD
				}
			}

			ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
			ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

			ce.Types[0] = (extrae_type_t) EventType::RUNTIME_STATE;
			ce.Values[0] = (extrae_value_t) NANOS_IDLE;

			if (Extrae::_detailLevel >= (int) THREADS_AND_CPUS_LEVEL) {
				if (Extrae::_traceAsThreads) {
					ce.Types[1] = (extrae_type_t) EventType::CPU;
					ce.Values[1] = (extrae_value_t) (cpu._id + 1);
					ce.Types[2] = (extrae_type_t) EventType::THREAD_NUMA_NODE;
					ce.Values[2] = (extrae_value_t) (cpu._NUMANode + 1);
				} else {
					ce.Types[1] = (extrae_type_t) EventType::THREAD;
					ce.Values[1] = (extrae_value_t) (threadId + 1);
				}
			}

			Extrae::emit_CombinedEvents ( &ce );
		}
	}

	inline void threadWillSuspend(__attribute__((unused)) external_thread_id_t threadId)
	{
		extrae_combined_events_t ce;

		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;

		ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
		ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

		ce.Types[0] = (extrae_type_t) EventType::RUNTIME_STATE;
		ce.Values[0] = (extrae_value_t) NANOS_NOT_RUNNING;

		Extrae::emit_CombinedEvents ( &ce );
	}

	inline void threadHasResumed(__attribute__((unused)) external_thread_id_t threadId)
	{
		extrae_combined_events_t ce;

		ce.HardwareCounters = 0;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 1;
		ce.nCommunications = 0;

		ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
		ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

		ce.Types[0] = (extrae_type_t) EventType::RUNTIME_STATE;
		ce.Values[0] = (extrae_value_t) NANOS_RUNTIME;

		Extrae::emit_CombinedEvents ( &ce );
	}

	static inline void extraeThreadWillShutdown()
	{
		if (Extrae::_traceAsThreads) {
			extrae_combined_events_t ce;

			ce.HardwareCounters = 0;
			ce.Callers = 0;
			ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
			ce.nEvents = 1;
			ce.nCommunications = 0;

			if (Extrae::_detailLevel >= (int) THREADS_AND_CPUS_LEVEL) {
				if (Extrae::_traceAsThreads) {
					ce.nEvents += 2; // CPU, THREAD_NUMA_NODE
				} else {
					ce.nEvents++; // THREAD
				}
			}

			ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
			ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

			ce.Types[0] = (extrae_type_t) EventType::RUNTIME_STATE;
			ce.Values[0] = (extrae_value_t) NANOS_SHUTDOWN;

			if (Extrae::_detailLevel >= (int) THREADS_AND_CPUS_LEVEL) {
				if (Extrae::_traceAsThreads) {
					ce.Types[1] = (extrae_type_t) EventType::CPU;
					ce.Values[1] = (extrae_value_t) 0;
					ce.Types[2] = (extrae_type_t) EventType::THREAD_NUMA_NODE;
					ce.Values[2] = (extrae_value_t) 0;
				} else {
					ce.Types[1] = (extrae_type_t) EventType::THREAD;
					ce.Values[1] = (extrae_value_t) 0;
				}
			}

			Extrae::emit_CombinedEvents ( &ce );
		}
	}

	inline void threadWillShutdown(__attribute__((unused)) external_thread_id_t threadId)
	{
		extraeThreadWillShutdown();
	}

	inline void threadWillShutdown()
	{
		extraeThreadWillShutdown();
	}

	inline void threadEnterBusyWait(__attribute__((unused)) busy_wait_reason_t reason)
	{
	}

	inline void threadExitBusyWait()
	{
	}
}


#endif // INSTRUMENT_EXTRAE_THREAD_MANAGEMENT_HPP
