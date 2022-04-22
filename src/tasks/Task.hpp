/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef TASK_HPP
#define TASK_HPP

#include <atomic>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <string>

#include <nanos6.h>

#include "hardware/device/DeviceEnvironment.hpp"
#include "hardware-counters/TaskHardwareCounters.hpp"
#include "lowlevel/SpinLock.hpp"
#include "scheduling/ReadyQueue.hpp"
#include "system/ompss/SpawnFunction.hpp"

#include <InstrumentTaskId.hpp>
#include <TaskDataAccesses.hpp>
#include <TaskDataAccessesInfo.hpp>
#include "cluster/offloading/OffloadedTaskId.hpp"

struct DataAccess;
struct DataAccessBase;
struct StreamFunctionCallback;
class ComputePlace;
class MemoryPlace;
class TaskStatistics;
class TasktypeData;
class WorkerThread;
class TasktypeData;

#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wunused-result"

// TODO: Mercurium defines this value hard coded, we must use a better approach
// and update it here and there in case of change.

namespace ExecutionWorkflow {
	class Step;
	class Workflow;
	class DataReleaseStep;
	class DataLinkStep;
};

namespace TaskOffloading {
	class ClusterTaskContext;
};

class Task {
public:
	enum {
		//! Flags added by the Mercurium compiler
		final_flag=0,
		if0_flag,
		taskloop_flag,
		taskfor_flag,
		wait_flag,
		preallocated_args_block_flag,
		lint_verified_flag,
		//! Flags added by the Nanos6 runtime. Note that
		//! these flags must be always declared after the
		//! Mercurium flags
		non_runnable_flag,
		stream_executor_flag,
		stream_flag,
		main_task_flag,
		remote_wrapper_flag,
		remote_flag,          // remote: offloaded to this node
		polling_flag,
		nonlocal_wait_flag,
		offloaded_flag,  // offloaded from this node
		total_flags
	};

	enum class nanos6_task_runtime_flag_t : size_t {
		nanos6_non_runnable_flag = (1 << non_runnable_flag),
		nanos6_stream_executor_flag = (1 << stream_executor_flag),
		nanos6_stream_flag = (1 << stream_flag),
		nanos6_main_task_flag = (1 << main_task_flag),
		nanos6_remote_wrapper_flag = (1 << remote_wrapper_flag),
		nanos6_remote_flag = (1 << remote_flag),
		nanos6_polling_flag = (1 << polling_flag)
	};

	static constexpr nanos6_task_constraints_t default_task_constraints_t = {
		.cost = 0,
		.stream = 0,
		.node = nanos6_cluster_no_hint
	};

	typedef long priority_t;

	typedef uint64_t deadline_t;

private:
	typedef std::bitset<total_flags> flags_t;

	void *_argsBlock;
	size_t _argsBlockSize;

	nanos6_task_info_t *_taskInfo;
	nanos6_task_invocation_info_t *_taskInvokationInfo;

	//! Number of children that are still not finished, +1 if not blocked
	std::atomic<int> _countdownToBeWokenUp;

	//! Number of children that are still alive (may have live references to data from this task),
	//! +1 for dependencies
	std::atomic<int> _removalCount;

	//! Task to which this one is closely nested
	Task *_parent;

	//! Offloaded ancestor task
	Task *_offloadedTask;

	//! Task priority
	priority_t _priority;

	//! Task deadline to start/resume in microseconds (zero by default)
	deadline_t _deadline;

	//! Scheduling hint used by the scheduler
	ReadyTaskHint _schedulingHint;

	//! DataReleaseStep related with this task
	SpinLock _releaseStepInfoLock;
	ExecutionWorkflow::DataReleaseStep *_dataReleaseStep;
	friend class ExecutionWorkflow::DataReleaseStep;

protected:
	//! The thread assigned to this task, nullptr if the task has finished (but possibly waiting its children)
	std::atomic<WorkerThread *> _thread;

	//! Accesses that may determine dependencies
	TaskDataAccesses _dataAccesses;

	// Need to get back to the task from TaskDataAccesses for instrumentation purposes
	friend struct TaskDataAccesses;

	//! Task flags
	flags_t _flags;

private:
	//! Number of pending predecessors
	std::atomic<int> _predecessorCount;

	//! An identifier for the task for the instrumentation
	Instrument::task_id_t _instrumentationTaskId;

	//! Compute Place where the task is running
	ComputePlace *_computePlace;

	//! MemoryPlace "attached" to the ComputePlace the Task is running on
	MemoryPlace *_memoryPlace;

	//! Device Specific data
	void *_deviceData;

	//! Device Environment
	DeviceEnvironment _deviceEnvironment;

	//! Number of internal and external events that prevent the release of dependencies
	std::atomic<int> _countdownToRelease;

	//! Execution workflow to execute this Task
	ExecutionWorkflow::Workflow *_workflow;

	//! At the moment we will store the Execution step of the task
	//! here in order to invoke it after previous asynchronous
	//! steps have been completed.
	ExecutionWorkflow::Step *_executionStep;

	//! Monitoring-related statistics about the task
	TaskStatistics *_taskStatistics;

	//! Hardware counter structures of the task
	TaskHardwareCounters _hwCounters;

	//! Cluster-related data for remote tasks
	TaskOffloading::ClusterTaskContext *_clusterContext;

	//! A pointer to the callback of the spawned function that created the
	//! task, used to trigger a callback from the appropriate stream function
	//! if the parent of this task is a StreamExecutor
	StreamFunctionCallback *_parentSpawnCallback;

	//! Nesting level of the task
	int _nestingLevel;

	//! Offloaded task ID
	OffloadedTaskId _offloadedTaskId;

	//! _clusterNode if set by call to setNode()
	nanos6_task_constraints_t *_constraints; // -1 is same as nanos6_cluster_no_hint, so not overridden by runtime

	bool _countAsImmovable;
	bool _countedAsImmovable;

public:
	inline Task(
		void *argsBlock,
		size_t argsBlockSize,
		nanos6_task_info_t *taskInfo,
		nanos6_task_invocation_info_t *taskInvokationInfo,
		Task *parent,
		Instrument::task_id_t instrumentationTaskId,
		size_t flags,
		const TaskDataAccessesInfo &taskAccessInfo,
		void *taskCountersAddress,
		void *taskStatistics
	);

	virtual inline void reinitialize(
		void *argsBlock,
		size_t argsBlockSize,
		nanos6_task_info_t *taskInfo,
		nanos6_task_invocation_info_t *taskInvokationInfo,
		Task *parent,
		Instrument::task_id_t instrumentationTaskId,
		size_t flags
	);

	virtual inline ~Task();

	//! Set the address of the arguments block
	inline void setArgsBlock(void *argsBlock)
	{
		_argsBlock = argsBlock;
	}

	//! Get the address of the arguments block
	inline void *getArgsBlock() const
	{
		return _argsBlock;
	}

	//! Get the arguments block size
	inline size_t getArgsBlockSize() const
	{
		return _argsBlockSize;
	}

	inline void setArgsBlockSize(size_t argsBlockSize)
	{
		_argsBlockSize = argsBlockSize;
	}

	inline nanos6_task_info_t *getTaskInfo() const
	{
		return _taskInfo;
	}

	inline nanos6_task_invocation_info_t *getTaskInvokationInfo() const
	{
		return _taskInvokationInfo;
	}

	//! Actual code of the task
	virtual inline void body(nanos6_address_translation_entry_t *translationTable = nullptr)
	{
		assert(_taskInfo->implementation_count == 1);
		assert(hasCode());
		assert(_taskInfo != nullptr);
		assert(!isTaskfor());
		_taskInfo->implementations[0].run(_argsBlock, (void *)&_deviceEnvironment, translationTable);
	}

	//! Check if the task has an actual body
	inline bool hasCode() const
	{
		assert(_taskInfo->implementation_count == 1); // TODO: temporary check for a single implementation
		assert(_taskInfo != nullptr);
		return (_taskInfo->implementations[0].run != nullptr); // TODO: solution until multiple implementations are allowed
	}

	//! \brief sets the thread assigned to tun the task
	//!
	//! \param in thread the thread that will run the task
	inline void setThread(WorkerThread *thread)
	{
		assert(thread != nullptr);
		assert(_thread == nullptr);
		_thread = thread;
	}

	//! \brief get the thread that runs or will run the task
	//!
	//! \returns the thread that runs or will run the task
	inline WorkerThread *getThread() const
	{
		return _thread;
	}

	//! \brief Add a nested task
	inline void addChild(__attribute__((unused)) Task *child)
	{
		_countdownToBeWokenUp.fetch_add(1, std::memory_order_relaxed);
		_removalCount.fetch_add(1, std::memory_order_relaxed);
	}

	//! \brief Remove a nested task (because it has finished)
	//!
	//! \returns true iff the change makes this task become ready
	inline bool finishChild() __attribute__((warn_unused_result))
	{
		const int countdown = (_countdownToBeWokenUp.fetch_sub(1, std::memory_order_relaxed) - 1);
		assert(countdown >= 0);
		return (countdown == 0);
	}

	//! \brief Remove a nested task (because it has been deleted)
	//!
	//! \returns true iff the change makes this task become disposable
	inline bool removeChild(__attribute__((unused)) Task *child) __attribute__((warn_unused_result))
	{
		const int countdown = (_removalCount.fetch_sub(1, std::memory_order_relaxed) - 1);
		assert(countdown >= 0);
		return (countdown == 0);
	}

	//! \brief Increase an internal counter to prevent the removal of the task
	inline void increaseRemovalBlockingCount()
	{
		_removalCount.fetch_add(1, std::memory_order_relaxed);
	}

	//! \brief Decrease an internal counter that prevents the removal of the task
	//!
	//! \returns true iff the change makes this task become ready or disposable
	inline bool decreaseRemovalBlockingCount()
	{
		const int countdown = (_removalCount.fetch_sub(1, std::memory_order_relaxed) - 1);
		assert(countdown >= 0);
		return (countdown == 0);
	}

	//! \brief Set the parent
	//! This should be used when the parent was not set during creation, and should have the parent in a state that allows
	//! adding this task as a child.
	//! \param parent inout the actual parent of the task
	inline void setParent(Task *parent)
	{
		assert(parent != nullptr);
		_parent = parent;
		_parent->addChild(this);
		_nestingLevel = _parent->getNestingLevel() + 1;

		// Find and set the offloader predecessor.  setParent is called in submitTask; so we need to
		// call set the _offloadedTask just here.
		Task *task = this;

		while (task->getParent() != nullptr) {
			if (task->getParent()->isNodeNamespace()) {
				// it is an offloaded task
				_offloadedTask = task;
				return;
			}
			task = task->getParent();
		}
	}

	//! \brief Get the parent into which this task is nested
	//!
	//! \returns the task into which one is closely nested, or null if this is the main task
	inline Task *getParent() const
	{
		return _parent;
	}

	//! \brief Remove the link between the task and its parent
	//!
	//! \returns true iff the change made the parent become ready or disposable
	inline bool unlinkFromParent() __attribute__((warn_unused_result))
	{
		if (_parent != nullptr) {
			return _parent->removeChild(this);
		} else {
			return (_removalCount == 0);
		}
	}

	//! \brief Get the task priority
	//!
	//! \returns the priority
	inline priority_t getPriority() const
	{
		return _priority;
	}

	//! \brief Compute the task priority defined by the user
	//!
	//! \returns whether the task has a user-defined priority
	inline bool computePriority()
	{
		assert(_taskInfo != nullptr);
		assert(_argsBlock != nullptr);

		if (_taskInfo->get_priority != nullptr) {
			_taskInfo->get_priority(_argsBlock, &_priority);
			return true;
		}
		// Leave the default priority
		return false;
	}

	//! \brief Indicates whether the task has deadline
	//!
	//! \returns whether the task has deadline
	inline bool hasDeadline() const
	{
		return (_deadline > 0);
	}

	//! \brief Get the task deadline (us) to start/resume
	//!
	//! \returns the task deadline in us
	inline deadline_t getDeadline() const
	{
		return _deadline;
	}

	//! \brief Set the task deadline (us) to start/resume
	//!
	//! \param deadline the new task deadline in us
	inline void setDeadline(deadline_t deadline)
	{
		_deadline = deadline;
	}

	//! \brief Get the task scheduling hint
	//!
	//! \returns the scheduling hint
	inline ReadyTaskHint getSchedulingHint() const
	{
		return _schedulingHint;
	}

	//! \brief Set the task scheduling hint
	//!
	//! \param hint the new scheduling hint
	inline void setSchedulingHint(ReadyTaskHint hint)
	{
		_schedulingHint = hint;
	}

	//! \brief Mark that the task has finished its execution
	//! It marks the task as finished and determines if the
	//! dependencies can be released. The release could be
	//! postponed due to uncompleted external events. It could
	//! also be postponed due to a wait clause, in which the
	//! last child task should release the dependencies
	//!
	//! Note: This should be called only from the body of the
	//! thread that has executed the task
	//!
	//! \param computePlace in the compute place of the calling thread
	//!
	//! \returns true if its dependencies can be released
	inline bool markAsFinished(ComputePlace *computePlace);

	//! \brief Mark that the dependencies of the task have been released
	//!
	//! \returns true if the task can be disposed
	inline bool markAsReleased() __attribute__((warn_unused_result))
	{
		assert(_thread == nullptr);
		assert(_computePlace == nullptr);
		return decreaseRemovalBlockingCount();
	}

	//! \brief Mark that all its child tasks have finished
	//! It marks that all children have finished and determines
	//! if the dependencies can be released. It completes the
	//! delay of the dependency release in case the task has a
	//! wait clause, however, some external events could be still
	//! uncompleted
	//!
	//! Note: This should be called when unlinking the last child
	//! task (i.e. the removal counter becomes zero)
	//!
	//! \param computePlace in the compute place of the calling thread
	//!
	//! \returns true if its depedencies can be released
	inline bool markAllChildrenAsFinished(ComputePlace *computePlace);

	inline bool allChildrenHaveFinished() const
	{
		return _countdownToRelease == 0;
	}

	//! \brief Mark it as blocked
	//!
	//! \returns true if the change makes the task become ready
	inline bool markAsBlocked()
	{
		const int countdown = (_countdownToBeWokenUp.fetch_sub(1, std::memory_order_relaxed) - 1);
		assert(countdown >= 0);
		return (countdown == 0);
	}

	//! \brief Mark it as unblocked
	//!
	//! \returns true if it does not have any children
	inline bool markAsUnblocked()
	{
		return (_countdownToBeWokenUp.fetch_add(1, std::memory_order_relaxed) == 0);
	}

	//! \brief Decrease the remaining count for unblocking the task
	//!
	//! \returns true if the change makes the task become ready
	inline bool decreaseBlockingCount()
	{
		int countdown = (_countdownToBeWokenUp.fetch_sub(1, std::memory_order_relaxed) - 1);
		assert(countdown >= 0);
		return (countdown == 0);
	}

	//! \brief Increase the remaining count for unblocking the task
	inline void increaseBlockingCount()
	{
		_countdownToBeWokenUp.fetch_add(1, std::memory_order_relaxed);
	}

	//! \brief Indicates whether it has finished
	inline bool hasFinished() const
	{
		if (_taskInfo->implementations[0].device_type_id != nanos6_host_device) {
			return (_computePlace == nullptr);
		}

		return (_thread == nullptr);
	}

	//! \brief Indicates if it can be woken up
	//! Note: The task must have been marked as blocked
	inline bool canBeWokenUp()
	{
		// assert(_thread != nullptr);
		return (_countdownToBeWokenUp == 0);
	}

	//! \brief Indicates if it does not have any children
	inline bool doesNotNeedToBlockForChildren()
	{
		return (_removalCount == 1);
	}

	//! \brief Prevent this task to be scheduled when it is unblocked
	//!
	//! \returns true if this task is still not unblocked
	inline bool disableScheduling()
	{
		int countdown = _countdownToBeWokenUp.load();
		assert(countdown >= 0);

		// If it is 0 (unblocked), do not increment
		while (countdown > 0
			&& !_countdownToBeWokenUp.compare_exchange_strong(countdown, countdown + 1)) {
		}

		return (countdown > 0);
	}

	//! \brief Enable scheduling again for this task
	//!
	//! \returns true if this task is unblocked
	inline bool enableScheduling()
	{
		const int countdown = (_countdownToBeWokenUp.fetch_sub(1, std::memory_order_relaxed) - 1);
		assert(countdown >= 0);
		return (countdown == 0);
	}

	inline int getPendingChildTasks() const
	{
		return _countdownToBeWokenUp.load(std::memory_order_relaxed) - 1;
	}

	//! \brief Retrieve the list of data accesses
	TaskDataAccesses const &getDataAccesses() const
	{
		return _dataAccesses;
	}

	//! \brief Retrieve the list of data accesses
	TaskDataAccesses &getDataAccesses()
	{
		return _dataAccesses;
	}

	//! \brief Increase the number of predecessors
	void increasePredecessors(int amount=1)
	{
		_predecessorCount += amount;
	}

	//! \brief Decrease the number of predecessors
	//! \returns true if the task becomes ready
	bool decreasePredecessors(int amount=1)
	{
		const int res = (_predecessorCount-= amount);
		assert(res >= 0);
		return (res == 0);
	}

	//! \brief Set or unset the final flag
	void setFinal(bool finalValue)
	{
		_flags[final_flag] = finalValue;
	}
	//! \brief Check if the task is final
	bool isFinal() const
	{
		return _flags[final_flag];
	}

	//! \brief Set or unset the if0 flag
	void setIf0(bool if0Value)
	{
		_flags[if0_flag] = if0Value;
	}
	//! \brief Check if the task is in if0 mode
	bool isIf0() const
	{
		return _flags[if0_flag];
	}

	//! \brief Set or unset the taskloop flag
	void setTaskloop(bool taskloopValue)
	{
		_flags[taskloop_flag] = taskloopValue;
	}
	//! \brief Check if the task is a taskloop
	bool isTaskloop() const
	{
		return _flags[taskloop_flag];
	}

	//! \brief Set or unset the taskfor flag
	void setTaskfor(bool taskforValue)
	{
		_flags[taskfor_flag] = taskforValue;
	}
	//! \brief Check if the task is a taskfor
	bool isTaskfor() const
	{
		return _flags[taskfor_flag];
	}

	inline bool isRunnable() const
	{
		return !_flags[non_runnable_flag];
	}

	//! \brief Set the wait behavior
	inline void setDelayedRelease(bool delayedReleaseValue)
	{
		_flags[wait_flag] = delayedReleaseValue;
		_flags[nonlocal_wait_flag] = false;
	}
	//! \brief Check if the task has the wait clause
	bool mustDelayRelease() const
	{
		return _flags[wait_flag];
	}

	//! \brief Complete the delay of the dependency release
	//! It completes the delay of the dependency release
	//! enforced by a wait clause
	void completeDelayedRelease()
	{
		assert(_flags[wait_flag]);
		_flags[wait_flag] = false;
	}

	//! \brief Set delayed release of only the non-locally propagated
	//! accesses; i.e. those that will require a release access message
	//! rather than those that are propagated in the namespace. Delayed
	//! release is beneficial as it may unfragment accesses to combine
	//! messages.
	inline void setDelayedNonLocalRelease()
	{
		_flags[nonlocal_wait_flag] = true;
		//! Non-local wait is implemented as a variant of the wait clause,
		//! so also set the normal wait flag.
		_flags[wait_flag] = true;
	}

	//! \brief Check whether delayed release is only for non-locally
	//! propagated accesses.
	inline bool delayedReleaseNonLocalOnly() const
	{
		assert(_flags[wait_flag]);
		return _flags[nonlocal_wait_flag];
	}


	inline bool hasPreallocatedArgsBlock() const
	{
		return _flags[preallocated_args_block_flag];
	}

	inline bool isSpawned() const
	{
		return SpawnFunction::isSpawned(_taskInfo);
	}

	inline bool isRemoteWrapper() const
	{
		return _flags[remote_wrapper_flag];
	}

	// Check if this is a remote task (offloaded to this node)
	inline bool isRemoteTask() const
	{
		const bool ret =_flags[remote_flag];
		assert(ret ? (_parent != nullptr) : true);
		return ret;
	}

	// Check if this is an offloaded task (from this node)
	inline bool isOffloadedTask() const
	{
		const bool ret =_flags[offloaded_flag];
		assert(ret ? (_parent != nullptr) : true);
		return ret;
	}

	inline bool isPolling() const
	{
		return _flags[polling_flag];
	}

	inline size_t getFlags() const
	{
		return _flags.to_ulong();
	}

	//! \brief Retrieve the instrumentation-specific task identifier
	inline Instrument::task_id_t getInstrumentationTaskId() const
	{
		return _instrumentationTaskId;
	}

	//! \brief Increase the counter of events
	inline void increaseReleaseCount(int amount = 1)
	{
		assert(_countdownToRelease > 0);

		_countdownToRelease += amount;
	}

	//! \brief Decrease the counter of events
	//!
	//! \returns true iff the dependencies can be released
	inline bool decreaseReleaseCount(int amount = 1)
	{
		const int count = (_countdownToRelease -= amount);
		assert(count >= 0);
		return (count == 0);
	}

	//! \brief Return the number of symbols on the task
	inline int getNumSymbols()
	{
		return _taskInfo->num_symbols;
	}

	inline ComputePlace *getComputePlace() const
	{
		return _computePlace;
	}
	inline void setComputePlace(ComputePlace *computePlace)
	{
		_computePlace = computePlace;
	}

	inline MemoryPlace *getMemoryPlace() const
	{
		return _memoryPlace;
	}
	inline void setMemoryPlace(MemoryPlace *memoryPlace)
	{
		_memoryPlace = memoryPlace;
	}
	inline bool hasMemoryPlace() const
	{
		return _memoryPlace != nullptr;
	}

	//! \brief Get the device type for which this task is implemented
	inline int getDeviceType()
	{
		return _taskInfo->implementations[0].device_type_id;
	}

	//! \brief Get the device subtype for which this task is implemented, TODO: device_subtype_id.
	inline int getDeviceSubType()
	{
		return 0;
	}

	inline void *getDeviceData()
	{
		return _deviceData;
	}
	inline void setDeviceData(void *deviceData)
	{
		_deviceData = deviceData;
	}

	inline DeviceEnvironment &getDeviceEnvironment()
	{
		return _deviceEnvironment;
	}

	//! \brief Set the Execution Workflow for this Task
	inline void setWorkflow(ExecutionWorkflow::Workflow *workflow)
	{
#ifndef NDEBUG
		// This is not enforced, but usefull when we try to execute the notification step more than
		// once.
		if (_workflow == nullptr) {
			assert(workflow != nullptr);
		} else {
			assert(workflow == nullptr);
		}
#endif // NDEBUG
		_workflow = workflow;
	}
	//! \brief Get the Execution Workflow of the Task
	inline ExecutionWorkflow::Workflow *getWorkflow() const
	{
		return _workflow;
	}

	inline void setExecutionStep(ExecutionWorkflow::Step *step)
	{
		_executionStep = step;
	}
	inline ExecutionWorkflow::Step *getExecutionStep() const
	{
		return _executionStep;
	}

	//! \brief Get a label that identifies the tasktype
	inline const std::string getLabel() const
	{
		if (_taskInfo != nullptr) {
			if (_taskInfo->implementations != nullptr) {
				if (_taskInfo->implementations->task_label != nullptr) {
					return std::string(_taskInfo->implementations->task_label);
				} else if (_taskInfo->implementations->declaration_source != nullptr) {
					return std::string(_taskInfo->implementations->declaration_source);
				}
			}

			// If the label is empty, use the invocation source
			return std::string(_taskInvokationInfo->invocation_source);
		} else if (_parent != nullptr) {
			return _parent->getLabel();
		}

		return "Unlabeled";
	}

	inline void setConstraints(nanos6_task_constraints_t *address)
	{
		assert(_constraints == nullptr || _constraints == address);
		assert(address != nullptr);
		_constraints = address;

		*_constraints = default_task_constraints_t;
	}

	inline void initConstraints()
	{
		if (_constraints == nullptr) {
			assert(isTaskforSource());
			return;
		}

		// We set and initialize constrains. This could be done in the constructor, but requires
		// many changes with no real benefit.
		if (_taskInfo != nullptr
			&& _taskInfo->implementations != nullptr
			&& _taskInfo->implementations->get_constraints != nullptr) {

			nanos6_task_constraints_t tmp;
			_taskInfo->implementations->get_constraints(_argsBlock, &tmp);

			if (_constraints->cost == default_task_constraints_t.cost) {
				_constraints->cost = tmp.cost;
			}
			if (_constraints->stream == default_task_constraints_t.stream) {
				_constraints->stream = tmp.stream;
			}
			if (_constraints->node == default_task_constraints_t.node) {
				_constraints->node = tmp.node;
			}
		}
	}


	const inline nanos6_task_constraints_t * getConstraints() const
	{
		assert(_constraints != nullptr);
		return _constraints;
	}

	inline void setNode(int node)
	{
		assert(_constraints != nullptr);
		_constraints->node = node;
	}

	//! \brief Get the task's statistics
	inline TaskStatistics *getTaskStatistics()
	{
		return _taskStatistics;
	}

	//! \brief Get the task's hardware counter structures
	inline TaskHardwareCounters &getHardwareCounters()
	{
		return _hwCounters;
	}

	inline void markAsRemote()
	{
		_flags[remote_flag] = true;
	}

	inline void unmarkAsRemote()
	{
		_flags[remote_flag] = false;
	}

	inline void markAsOffloaded()
	{
		_flags[offloaded_flag] = true;
	}

	inline void setClusterContext(TaskOffloading::ClusterTaskContext *clusterContext)
	{
		assert(clusterContext != nullptr);
		_clusterContext = clusterContext;
	}

	inline TaskOffloading::ClusterTaskContext *getClusterContext() const
	{
		return _clusterContext;
	}

	inline bool isStreamExecutor() const
	{
		return _flags[stream_executor_flag];
	}

	inline bool isStreamTask() const
	{
		return _flags[stream_flag];
	}

	inline void setParentSpawnCallback(StreamFunctionCallback *callback)
	{
		_parentSpawnCallback = callback;
	}

	inline StreamFunctionCallback *getParentSpawnCallback() const
	{
		return _parentSpawnCallback;
	}

	inline void markAsMainTask()
	{
		_flags[main_task_flag] = true;
	}

	inline bool isMainTask() const
	{
		return _flags[main_task_flag];
	}

	inline TasktypeData *getTasktypeData() const
	{
		if (_taskInfo != nullptr) {
			return (TasktypeData *) _taskInfo->task_type_data;
		}

		return nullptr;
	}

	virtual inline void registerDependencies(bool = false)
	{
		_taskInfo->register_depinfo(_argsBlock, nullptr, this);
	}

	virtual inline bool isDisposable() const
	{
		return true;
	}

	virtual inline bool isTaskloopSource() const
	{
		return false;
	}

	virtual inline bool isTaskloopFor() const
	{
		return false;
	}

	virtual inline bool isTaskloopOffloader() const
	{
		return false;
	}

	virtual inline bool isTaskforCollaborator() const
	{
		return false;
	}

	virtual inline bool isTaskforSource() const
	{
		return false;
	}

	inline int getNestingLevel() const
	{
		return _nestingLevel;
	}

	virtual inline void increaseMaxChildDependencies()
	{
	}

	bool isNodeNamespace() const
	{
		const bool ret = _flags[remote_wrapper_flag] && _flags[polling_flag];

		if (ret) {
			assert(_flags[preallocated_args_block_flag]);
			return true;
		}

		return false;
	}

	inline bool isRemoteTaskInNamespace() const
	{
		// isRemoteTask asserts the parent is non null when true
		return isRemoteTask() && _parent && _parent->isNodeNamespace();
	}

	//! Set the DataReleaseStep of the task. The task must not
	//! have a DataReleaseStep already
	void setDataReleaseStep(ExecutionWorkflow::DataReleaseStep *step)
	{
		assert(!hasDataReleaseStep());
		assert(step != nullptr);

		_dataReleaseStep = step;
	}

	//! Unset the DataReleaseStep of the access. The access must
	//! have a DataReleaseStep already
	void unsetDataReleaseStep()
	{
		assert(hasDataReleaseStep());

		_dataReleaseStep = nullptr;
	}

	//! Get the DataReleaseStep of the access.
	ExecutionWorkflow::DataReleaseStep *getDataReleaseStep() const
	{
		return _dataReleaseStep;
	}

	//! Check if the access has a DataReleaseStep
	bool hasDataReleaseStep() const
	{
		return (_dataReleaseStep != nullptr);
	}


	Task *getOffloadedPredecesor() const
	{
		return _offloadedTask;
	}

	OffloadedTaskId getOffloadedTaskId() const
	{
		return _offloadedTaskId;
	}

	void setOffloadedTaskId(OffloadedTaskId taskId)
	{
		_offloadedTaskId = taskId;
	}

	void dontCountAsImmovable(void)
	{
		_countAsImmovable = false;
	}

	bool getCountAsImmovable(void) const
	{
		return _countAsImmovable;
	}

	void setCountedAsImmovable(void)
	{
		_countedAsImmovable = true;
	}

	bool getCountedAsImmovable(void) const
	{
		return _countedAsImmovable;
	}
};


#pragma GCC diagnostic push


#endif // TASK_HPP

