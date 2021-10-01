/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef EXECUTION_STEP_HPP
#define EXECUTION_STEP_HPP

#include <functional>

#include "dependencies/DataAccessType.hpp"
#include "lowlevel/SpinLock.hpp"

#include <mutex>
#include <vector>
#include <DataAccessRegion.hpp>
#include "WriteID.hpp"

#include <SatisfiabilityInfo.hpp>
#include <DataSendInfo.hpp>

struct DataAccess;
class MemoryPlace;
class Task;
class SatisfiabilityInfoMap;

namespace ExecutionWorkflow {

	// NOTE: objects of this class self-destruct when they finish
	class Step {
	protected:
		//! pending previous steps
		int _countdownToRelease;

		//! list of successor Steps
		std::vector<Step *> _successors;

		//! lock to protect access to _successors
		//! TODO: maybe it makes sense to create an AtomicVector type
		SpinLock _lock;

	public:
		Step() : _countdownToRelease(0), _successors(), _lock()
		{
		}

		virtual ~Step()
		{
		}

		//! \brief add one pending predecessor to the Step
		inline void addPredecessor()
		{
			std::lock_guard<SpinLock> guard(_lock);
			++_countdownToRelease;
		}

		//! \brief Add a successor Step
		inline void addSuccessor(Step *step)
		{
			std::lock_guard<SpinLock> guard(_lock);
			_successors.push_back(step);
		}

		//! \brief Decrease the number of predecessors and start the Step execution
		//!        if the Step became ready.
		//!
		//! \returns true if the next Step is ready to start.
		inline bool release()
		{
			std::lock_guard<SpinLock> guard(_lock);
			--_countdownToRelease;
			assert(_countdownToRelease >= 0);

			return (_countdownToRelease == 0);
		}

		//! \brief Release successor steps
		inline void releaseSuccessors()
		{
			/* Commenting out the following lock, because it is
			 * actually not needed and it might lead to a deadlock.
			 * At this point, the Workflow is created and the
			 * _successors vector will not be further modified. */

			/*
			 * Actually it must not take the lock because
			 * ClusterDataLinkStep::start() calls it holding the
			 * lock.
			 */
			//std::lock_guard<SpinLock> guard(_lock);
			for (auto step: _successors) {
				if (step->release()) {
					step->start();
				}
			}
		}

		//! \brief Returns true if Step is ready to run
		inline bool ready() const
		{
			return (_countdownToRelease == 0);
		}

		//! \brief start the execution of a Step
		virtual inline void start()
		{
			releaseSuccessors();
			delete this;
		}
	};

	class DataLinkStep : public Step {
	protected:
		//! The number of bytes that this Step has to link
		std::atomic<size_t> _bytesToLink;

	public:
		//! \brief Create a DataLinkStep
		//!
		//! Create a DataLinkStep associated with a DataAccess. This is
		//! meant to be used in cases where we need to link information
		//! regarding the access, to a matching access on a device.
		//!
		//! \param[in] access is the DataAccess this DataLinkStep is
		//!		associated with. The access is a non-const
		//!		pointer, because the constructor might need to
		//!		set the corresponding field in the DataAccess
		//!		object.
		DataLinkStep(DataAccess *access);

		virtual inline void linkRegion(
			const DataAccess *,
			bool /*read*/,
			bool /*write*/,
			TaskOffloading::SatisfiabilityInfoMap &, /*hpDependencyData*/
			TaskOffloading::DataSendRegionInfoMap &
		) {
			//clusterCout << "Link: (" << access->getOriginator()->getLabel() <<"): " << region << std::endl;
		}
	};

	class DataReleaseStep : public Step {
	protected:
		//! type of the DataAccess
		DataAccessType _type;

		//! is the DataAccess weak?
		bool _weak;

		//! The number of bytes that this Step has to release
		std::atomic<size_t> _bytesToRelease;

		Task * const _task;

		SpinLock &_infoLock;

	public:

		//! \brief Create a DataReleaseStep
		//!
		//! Create a DataReleaseStep associated with a DataAccess. This
		//! is meant to be used in cases where we need to notify, once
		//! a condition has been met, that the data of the
		//! corresponding access are released from the device we have
		//! offloaded the originator Task.
		//!
		//! \param[in] access is the DataAccess this DataReleaseStep is
		//!		associated with. The access is a non-const
		//!		pointer, because the constructor might need to
		//!		set the corresponding field in the DataAccess
		//!		object
		DataReleaseStep(Task *task);

		virtual inline ~DataReleaseStep()
		{}

		virtual inline void addToReleaseList(DataAccess const *)
		{}

		virtual inline void releasePendingAccesses(bool)
		{}

		//! \brief Check if a DataAccess is ready to release data
		//!
		//! Whether a DataAccess is ready to release data or not
		//! depends on the kind of task which originates it (e.g.
		//! cluster, CUDA, etc.).
		//!
		//! This method serves the purpose to provide a way for each
		//! ExecutionWorkflow implementation (e.g. cluster, CUDA) that
		//! uses the DataReleaseStep to define when an access, that has
		//! an associated DataReleaseStep, is ready to release data.
		//!
		//! \param[in] access is the DataAccess which is related with
		//!		this Step and we check whether it is ready to
		//!		release data
		//!
		//! \returns true if access is ready to release data
		virtual inline bool checkDataRelease(__attribute__((unused))DataAccess const *access) const
		{
			return false;
		}

		virtual void addAccess(DataAccess *)
		{
		}
	};

	class NotificationStep : public Step {
		std::function<void ()> const _callback;
	public:
		NotificationStep(std::function<void ()> const &callback)
			: Step(), _callback(callback)
		{
		}

		//! start the execution of the Step
		inline void start() override
		{
			if (_callback) {
				_callback();
			}

			releaseSuccessors();
			delete this;
		}
	};

}

#endif /* EXECUTION_STEP_HPP */
