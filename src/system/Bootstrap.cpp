/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <assert.h>
#include <config.h>
#include <dlfcn.h>
#include <iostream>

#include <nanos6.h>
#include <nanos6/bootstrap.h>

#include "LeaderThread.hpp"
#include "MemoryAllocator.hpp"
#include "executors/threads/CPUManager.hpp"
#include "executors/threads/ThreadManager.hpp"
#include "hardware/HardwareInfo.hpp"
#include "hardware-counters/HardwareCounters.hpp"
#include "lowlevel/TurboSettings.hpp"
#include "lowlevel/threads/ExternalThread.hpp"
#include "lowlevel/threads/ExternalThreadGroup.hpp"
#include "monitoring/Monitoring.hpp"
#include "scheduling/Scheduler.hpp"
#include "support/config/ConfigCentral.hpp"
#include "support/config/ConfigChecker.hpp"
#include "system/APICheck.hpp"
#include "system/RuntimeInfoEssentials.hpp"
#include "system/Throttle.hpp"
#include "system/ompss/SpawnFunction.hpp"
#include "tasks/StreamManager.hpp"

#include <ClusterManager.hpp>
#include <ClusterStats.hpp>
#include <DependencySystem.hpp>
#include <InstrumentInitAndShutdown.hpp>
#include <InstrumentThreadManagement.hpp>

// This is memory allocator specific
bool MemoryAllocator::init = false;

static ExternalThread *mainThread = nullptr;

void nanos6_shutdown(void);

int nanos6_can_run_main(void)
{
	return ClusterManager::isMasterNode();
}

void nanos6_register_node(void (*shutdown_callback)(void *), void *callback_args)
{
	assert(ClusterManager::isMasterNode() || shutdown_callback != nullptr);
	ClusterManager::initClusterNamespace(shutdown_callback, callback_args);
}

// Before main started
void nanos6_preinit(int argc, char **argv)
{
	if (!nanos6_api_has_been_checked_successfully()) {
		int *_nanos6_exit_with_error_ptr = (int *) dlsym(nullptr, "_nanos6_exit_with_error");
		if (_nanos6_exit_with_error_ptr != nullptr) {
			*_nanos6_exit_with_error_ptr = 1;
		}

		FatalErrorHandler::failIf(
			!nanos6_api_has_been_checked_successfully(),
			"this executable was compiled for a different Nanos6 version. Please recompile and link it."
		);
	}

	// Initialize all runtime options if needed
	ConfigCentral::initializeOptionsIfNeeded();

	// Enable special flags for turbo mode
	TurboSettings::initialize();

	RuntimeInfoEssentials::initialize();

	// Pre-initialize Hardware Counters and Monitoring before hardware
	HardwareCounters::preinitialize();
	Monitoring::preinitialize();
	HardwareInfo::initialize();

	ClusterManager::initialize(argc, argv);

	 // CPUManager::preinitialize() must be after ClusterManager::initialize() for ClusterHybridManager::getInitialCPUMask
	CPUManager::preinitialize();

	// Finish Hardware counters and Monitoring initialization after CPUManager
	HardwareCounters::initialize();
	Monitoring::initialize();
	MemoryAllocator::initialize();
	Throttle::initialize();
	Scheduler::initialize();
	ExternalThreadGroup::initialize();

	Instrument::initialize();
	ClusterManager::initialize2();  // must be after Instrument::initialize()
	mainThread = new ExternalThread("main-thread");
	mainThread->preinitializeExternalThread();

	mainThread->initializeExternalThread(/* already preinitialized */ false);

	// Register mainThread so that it will be automatically deleted
	// when shutting down Nanos6
	ExternalThreadGroup::registerExternalThread(mainThread);
	Instrument::threadHasResumed(mainThread->getInstrumentationId());
	ClusterStats::createdThread(mainThread);
	ClusterStats::threadHasResumed(mainThread);

	ThreadManager::initialize();
	DependencySystem::initialize();

	// Retrieve the virtual CPU for the leader thread
	CPU *leaderThreadCPU = CPUManager::getLeaderThreadCPU();
	assert(leaderThreadCPU != nullptr);

	LeaderThread::initialize(leaderThreadCPU);

	CPUManager::initialize();

	Instrument::nanos6_preinit_finished();

	// Assert config conditions if any
	ConfigChecker::assertConditions();
}

// After main started
void nanos6_init(void)
{
	ClusterManager::postinitialize();
	Instrument::threadWillSuspend(mainThread->getInstrumentationId());
	ClusterStats::threadWillSuspend(mainThread);
	StreamManager::initialize();
}


void nanos6_shutdown(void)
{
	Instrument::threadHasResumed(mainThread->getInstrumentationId());
	ClusterStats::threadHasResumed(mainThread);
	Instrument::threadWillShutdown(mainThread->getInstrumentationId());

	while (SpawnFunction::_pendingSpawnedFunctions > 0) {
		// Wait for spawned functions to fully end
	}

	StreamManager::shutdown();
	LeaderThread::shutdown();

	// Signal the shutdown to all CPUs and finalize threads
	ClusterManager::shutdownPhase1();
	CPUManager::shutdownPhase1();
	ThreadManager::shutdownPhase1();

	Instrument::shutdown();

	// Delete spawned functions task infos
	SpawnFunction::shutdown();

	// Delete the worker threads
	// NOTE: AFTER Instrument::shutdown since it may need thread info!
	ThreadManager::shutdownPhase2();
	CPUManager::shutdownPhase2();

	// Delete all registered external threads, including mainThread
	ExternalThreadGroup::shutdown();

	Monitoring::shutdown();
	HardwareCounters::shutdown();
	Throttle::shutdown();

	Scheduler::shutdown();

	ClusterManager::shutdownPhase2();

	HardwareInfo::shutdown();
	MemoryAllocator::shutdown();
	RuntimeInfoEssentials::shutdown();
	TurboSettings::shutdown();
}
