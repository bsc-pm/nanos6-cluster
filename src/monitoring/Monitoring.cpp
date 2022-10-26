/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2021 Barcelona Supercomputing Center (BSC)
*/

#include <config.h>
#include <fstream>

#include "CPUMonitor.hpp"
#include "Monitoring.hpp"
#include "MonitoringSupport.hpp"
#include "TaskMonitor.hpp"
#include "TasktypeStatistics.hpp"
#include "RuntimeStateMonitor.hpp"
#include "executors/threads/CPUManager.hpp"
#include "hardware-counters/HardwareCounters.hpp"
#include "hardware-counters/SupportedHardwareCounters.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "support/JsonFile.hpp"
#include "tasks/Task.hpp"
#include "tasks/TaskInfo.hpp"
#include "ClusterManager.hpp"
#include "executors/threads/CPU.hpp"
#include "executors/threads/WorkerThread.hpp"


ConfigVariable<bool> Monitoring::_enabled("monitoring.enabled");
ConfigVariable<bool> Monitoring::_verbose("monitoring.verbose");
ConfigVariable<bool> Monitoring::_wisdomEnabled("monitoring.wisdom");
bool Monitoring::_forceRuntimeStateMonitor(false);
bool Monitoring::_taskMonitorEnabled(false);
bool Monitoring::_cpuMonitorEnabled(false);
bool Monitoring::_runtimeStateEnabled(false);
ConfigVariable<std::string> Monitoring::_outputFile("monitoring.verbose_file");
JsonFile *Monitoring::_wisdom(nullptr);
CPUMonitor *Monitoring::_cpuMonitor(nullptr);
TaskMonitor *Monitoring::_taskMonitor(nullptr);
size_t Monitoring::_predictedCPUUsage(0);


//    MONITORING    //

void Monitoring::enableRuntimeStateMonitor()
{
	_forceRuntimeStateMonitor = true;
}

void Monitoring::preinitialize()
{
	_taskMonitorEnabled = false;
	_runtimeStateEnabled = false;
	_cpuMonitorEnabled = false;
	if (_enabled) {
		ConfigVariableVector<std::string> monitoringAreas("monitoring.areas");
		for (auto area : monitoringAreas) {
			std::transform(area.begin(), area.end(), area.begin(), ::tolower);
			if (area == "all") {
				_taskMonitorEnabled = true;
				_runtimeStateEnabled = true;
				_cpuMonitorEnabled = true;
			} else if (area == "taskmonitor") {
				_taskMonitorEnabled = true;
			} else if (area == "!taskmonitor") {
				_taskMonitorEnabled = false;
			} else if (area == "cpumonitor") {
				_cpuMonitorEnabled = true;
			} else if (area == "!cpumonitor") {
				_cpuMonitorEnabled = false;
			} else if (area == "runtimestate") {
				_runtimeStateEnabled = true;
			} else if (area == "!runtimestate") {
				_runtimeStateEnabled = false;
				FatalErrorHandler::failIf(
					_forceRuntimeStateMonitor,
					"runtimestate monitoring is always needed for OmpSs-2@Cluster+DLB");
			} else {
				std::cerr << "Warning: ignoring unknown '" << area << "' monitoring" << std::endl;
			}
		}
	} else {
		_wisdomEnabled.setValue(false);
		if (_forceRuntimeStateMonitor) {
			// runtimestate monitoring is always needed for OmpSs-2@Cluster+DLB
			_enabled.setValue(true);
			_verbose.setValue(false);
			_runtimeStateEnabled = true;
		}
	}

	if (_taskMonitorEnabled) {
		assert(_enabled);
		// Create the task monitor before the CPUManager is initialized.
		// This is due to per-CPU preallocated taskfors needing task monitoring
		// to be enabled before they are constructed
		_taskMonitor = new TaskMonitor();
		assert(_taskMonitor != nullptr);
	}

	if (_wisdomEnabled) {
		// Try to load data from previous executions
		loadMonitoringWisdom();
	}
}

void Monitoring::initialize()
{
	// Make sure the CPUManager is already preinitialized before this
	assert(CPUManager::isPreinitialized());

	if (_cpuMonitorEnabled) {
		assert(_enabled);
		// Create the CPU monitor
		_cpuMonitor = new CPUMonitor();
		assert(_cpuMonitor != nullptr);
	}

	if (_runtimeStateEnabled) {
		assert(_enabled);
		RuntimeStateMonitor::initialize();
	}
}

void Monitoring::shutdown()
{
	if (_enabled) {
		if (_wisdomEnabled) {
			// Store monitoring data for future executions
			storeMonitoringWisdom();
		}

		if (_verbose) {
			displayStatistics();
		}

		// Delete all predictors and monitors
		if (_cpuMonitorEnabled) {
			assert(_cpuMonitor != nullptr);
			delete _cpuMonitor;
			_cpuMonitor = nullptr;
		}
		if (_taskMonitorEnabled) {
			assert(_taskMonitor != nullptr);
			delete _taskMonitor;
			_taskMonitor = nullptr;
		}
		_enabled.setValue(false);
	}
}


//    TASKS    //

void Monitoring::taskCreated(Task *task)
{
	if (_taskMonitorEnabled) {
		assert(_enabled);
		assert(task != nullptr);
		assert(_taskMonitor != nullptr);

		TaskStatistics *taskStatistics = task->getTaskStatistics();
		assert(taskStatistics != nullptr);

		// Construct the object with the reserved space
		void *innerAllocationAddress = (char *) taskStatistics + sizeof(TaskStatistics);
		new (taskStatistics) TaskStatistics(innerAllocationAddress);

		// Populate task statistic structures and predict metrics
		Task *parent = task->getParent();
		_taskMonitor->taskCreated(task, parent);
	}
}

void Monitoring::taskReinitialized(Task *task)
{
	if (_taskMonitorEnabled) {
		assert(_enabled);
		assert(_taskMonitor != nullptr);

		// Reset task statistics
		_taskMonitor->taskReinitialized(task);
	}
}

void Monitoring::taskChangedStatus(Task *task, monitoring_task_status_t newStatus)
{
	if (_taskMonitorEnabled) {
		assert(_enabled);
		assert(_taskMonitor != nullptr);

		// Start timing for the appropriate stopwatch
		_taskMonitor->taskStarted(task, newStatus);
	}
	if (_runtimeStateEnabled) {
		assert(_enabled);
		RuntimeStateMonitor::taskChangedStatus(task, newStatus);
	}
}

void Monitoring::taskCompletedUserCode(Task *task)
{
	if (_taskMonitorEnabled) {
		assert(_enabled);
		assert(_taskMonitor != nullptr);

		// Account the task's elapsed execution for predictions
		_taskMonitor->taskCompletedUserCode(task);
	}
}

void Monitoring::taskFinished(Task *task)
{
	if (_taskMonitorEnabled) {
		assert(_enabled);
		assert(_taskMonitor != nullptr);

		// Mark task as completely executed
		_taskMonitor->taskFinished(task);
	}
}


//    CPUS    //

void Monitoring::cpuBecomesIdle(int cpuId)
{
	if (_cpuMonitorEnabled) {
		assert(_enabled);
		assert(_cpuMonitor != nullptr);

		_cpuMonitor->cpuBecomesIdle(cpuId);
	}
	if (_runtimeStateEnabled) {
		RuntimeStateMonitor::cpuBecomesIdle(cpuId);
	}
}

void Monitoring::cpuBecomesActive(int cpuId)
{
	if (_cpuMonitorEnabled) {
		assert(_enabled);
		assert(_cpuMonitor != nullptr);

		_cpuMonitor->cpuBecomesActive(cpuId);
	}
	if (_runtimeStateEnabled) {
		RuntimeStateMonitor::cpuBecomesActive(cpuId);
	}
}

void Monitoring::cpuHintAsIdle(int cpuId)
{
	if (_runtimeStateEnabled) {
		RuntimeStateMonitor::cpuHintAsIdle(cpuId);
	}
}


//    PREDICTORS    //

size_t Monitoring::getPredictedCPUUsage(size_t time)
{
	if (_enabled) {
		double currentWorkload = 0.0;
		size_t currentActiveInstances = 0;
		size_t currentPredictionlessInstances = 0;
		TaskInfo::processAllTasktypes(
			[&](const std::string &, const std::string &, TasktypeData &tasktypeData) {
				TasktypeStatistics &statistics = tasktypeData.getTasktypeStatistics();
				Chrono completedChrono(statistics.getCompletedTime());
				double completedTime = ((double) completedChrono);
				size_t accumulatedCost = statistics.getAccumulatedCost();
				double accumulatedTime = statistics.getTimingPrediction(accumulatedCost);

				if (accumulatedTime > completedTime) {
					currentWorkload += (accumulatedTime - completedTime);
					currentActiveInstances += statistics.getTimingNumInstances();
					currentPredictionlessInstances += statistics.getNumPredictionlessInstances();
				}
			}
		);

		// At least one CPU, or if there are any predictionless instances, that number
		size_t predictedUsage = (currentPredictionlessInstances) ? currentPredictionlessInstances : 1;

		// Add the minimum between the number of tasks with prediction, and
		// the current workload in time divided by the required time
		predictedUsage += (size_t) ((double) currentWorkload / (double) time);
		predictedUsage = std::min(predictedUsage, (size_t) CPUManager::getAvailableCPUs());
		_predictedCPUUsage = predictedUsage;

		return predictedUsage;
	}

	return 0;
}

double Monitoring::getPredictedElapsedTime()
{
	if (_enabled) {
		assert(_cpuMonitor != nullptr);

		double currentWorkload = 0.0;
		TaskInfo::processAllTasktypes(
			[&](const std::string &, const std::string &, TasktypeData &tasktypeData) {
				TasktypeStatistics &statistics = tasktypeData.getTasktypeStatistics();
				Chrono completedChrono(statistics.getCompletedTime());
				double completedTime = ((double) completedChrono);
				size_t accumulatedCost = statistics.getAccumulatedCost();
				double accumulatedTime = statistics.getTimingPrediction(accumulatedCost);

				if (accumulatedTime > completedTime) {
					currentWorkload += (accumulatedTime - completedTime);
				}
			}
		);

		// Check how active CPUs currently are
		double currentCPUActiveness = _cpuMonitor->getTotalActiveness();

		// Check if the elapsed time substracted from the predictions underflows
		return (currentWorkload < 0.0 ? 0.0 : (currentWorkload / currentCPUActiveness));
	}

	return 0.0;
}


//    PRIVATE METHODS    //

void Monitoring::displayStatistics()
{

	// Try opening the output file
	std::ios_base::openmode openMode = std::ios::out;

	// In cluster mode
	std::string monitorFilename = _outputFile.getValue();
	size_t idx = monitorFilename.find("%N");
	if (idx != std::string::npos) {
		if (ClusterManager::inClusterMode()) {
			// Expand %N to the cluster node index
			std::stringstream ss;
			ss << ClusterManager::getCurrentClusterNode()->getIndex();
			std::string nodenum = ss.str();
			monitorFilename.replace(idx, /* length */ 2, nodenum);
		} else {
			// Remove %N
			monitorFilename.replace(idx, /* length */ 2, "");
		}
	}

	std::ofstream output(monitorFilename, openMode);
	FatalErrorHandler::warnIf(
		!output.is_open(),
		"Could not create or open the verbose file: ", _outputFile.getValue(), ". Using standard output."
	);

	// Retrieve statistics from every monitor
	std::stringstream outputStream;

	if (_taskMonitorEnabled) {
		assert(_taskMonitor != nullptr);
		_taskMonitor->displayStatistics(outputStream);
	}

	if (_cpuMonitorEnabled) {
		assert(_cpuMonitor != nullptr);
		_cpuMonitor->displayStatistics(outputStream);
	}

	if (_runtimeStateEnabled) {
		RuntimeStateMonitor::displayStatistics(outputStream);
	}

	if (output.is_open()) {
		output << outputStream.str();
		output.close();
	} else {
		std::cout << outputStream.str();
	}
}

void Monitoring::loadMonitoringWisdom()
{
	// Create a representation of the system file as a JsonFile
	_wisdom = new JsonFile("./.nanos6-monitoring-wisdom.json");
	assert(_wisdom != nullptr);

	// Try to populate the JsonFile with the system file's data
	_wisdom->loadData();

	// Navigate through the file and extract metrics of each tasktype
	_wisdom->getRootNode()->traverseChildrenNodes(
		[&](const std::string &label, const JsonNode<> &metricsNode) {
			// For each tasktype in the file, process all current registered
			// tasktypes to check if we must copy the wisdom data into them
			TaskInfo::processAllTasktypes(
				[&](const std::string &taskLabel, const std::string &, TasktypeData &tasktypeData) {
					if (taskLabel == label) {
						// Labels coincide, first copy Monitoring data
						if (metricsNode.dataExists("NORMALIZED_COST")) {
							double metricValue = 0.0;
							bool converted = metricsNode.getData("NORMALIZED_COST", metricValue);
							if (converted) {
								TasktypeStatistics &tasktypeStatistics = tasktypeData.getTasktypeStatistics();
								tasktypeStatistics.insertNormalizedTime(metricValue);
							}
						}

						// Next, copy Hardware Counters data if existent
						const std::vector<HWCounters::counters_t> &enabledCounters =
							HardwareCounters::getEnabledCounters();
						for (size_t i = 0; i < enabledCounters.size(); ++i) {
							HWCounters::counters_t counterType = enabledCounters[i];
							std::string metricLabel(HWCounters::counterDescriptions[counterType]);
							if (metricsNode.dataExists(metricLabel)) {
								double metricValue = 0.0;
								bool converted = metricsNode.getData(metricLabel, metricValue);
								if (converted) {
									TasktypeStatistics &tasktypeStatistics = tasktypeData.getTasktypeStatistics();
									tasktypeStatistics.insertNormalizedCounter(i, metricValue);
								}
							}
						}
					}
				}
			);
		}
	);
}

void Monitoring::storeMonitoringWisdom()
{
	assert(_wisdom != nullptr);

	JsonNode<> *rootNode = _wisdom->getRootNode();
	assert(rootNode != nullptr);

	// A vector of nodes to save
	std::vector<std::pair<std::string, JsonNode<double>>> nodesToSave;

	// Process all the tasktypes and gather Monitoring and Hardware Counters metrics
	TaskInfo::processAllTasktypes(
		[&](const std::string &taskLabel, const std::string &, TasktypeData &tasktypeData) {
			// If the file already contains this tasktype as a node, retreive
			// its inner node instead of creating a new one
			JsonNode<double> tasktypeNode;
			if (rootNode->childNodeExists(taskLabel)) {
				tasktypeNode = rootNode->getChildNode(taskLabel);
			}

			// Retreive monitoring statistics
			TasktypeStatistics &tasktypeStatistics = tasktypeData.getTasktypeStatistics();
			double value = tasktypeStatistics.getTimingRollingAverage();
			if (tasktypeNode.dataExists("NORMALIZED_COST")) {
				tasktypeNode.replaceData("NORMALIZED_COST", value);
			} else {
				tasktypeNode.addData("NORMALIZED_COST", value);
			}

			// Retreive hardware counter metrics
			const std::vector<HWCounters::counters_t> &enabledCounters =
				HardwareCounters::getEnabledCounters();
			for (size_t i = 0; i < enabledCounters.size(); ++i) {
				double counterValue = tasktypeStatistics.getCounterRollingAverage(i);
				if (counterValue >= 0.0) {
					std::string counterLabel(HWCounters::counterDescriptions[enabledCounters[i]]);
					if (tasktypeNode.dataExists(counterLabel)) {
						tasktypeNode.replaceData(counterLabel, counterValue);
					} else {
						tasktypeNode.addData(counterLabel, counterValue);
					}
				}
			}

			// Finally, add the new or updated tasktype node
			nodesToSave.push_back(std::make_pair(taskLabel, tasktypeNode));
		}
	);

	// Clear the wisdom file
	_wisdom->clearFile();

	// Save all the new/updated nodes in the file
	for (size_t i = 0; i < nodesToSave.size(); ++i) {
		rootNode->addChildNode(
			nodesToSave[i].first,
			nodesToSave[i].second
		);
	}

	// Store the data from the JsonFile in the system file
	_wisdom->storeData();

	// Delete the file as it is no longer needed
	delete _wisdom;
}

void Monitoring::enterBlockCurrentTask(Task *task, bool fromUserCode)
{
	if (_runtimeStateEnabled) {
		assert(_enabled);
		RuntimeStateMonitor::enterBlockCurrentTask(task, fromUserCode);
	}
}

void Monitoring::exitBlockCurrentTask(Task *task, bool fromUserCode)
{
	if (_runtimeStateEnabled) {
		assert(_enabled);
		RuntimeStateMonitor::exitBlockCurrentTask(task, fromUserCode);
	}
}

void Monitoring::enterWaitFor(CPU *cpu)
{
	if (_runtimeStateEnabled) {
		assert(_enabled);
		RuntimeStateMonitor::enterWaitFor(cpu->getIndex());
	}
}

void Monitoring::enterWaitForIf0Task(CPU *cpu)
{
	if (_runtimeStateEnabled) {
		assert(_enabled);
		RuntimeStateMonitor::enterWaitFor(cpu->getIndex());
	}
}
