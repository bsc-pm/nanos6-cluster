/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#include "tasks/Task.hpp"

#include "CommutativeScoreboard.hpp"
#include "TaskDataAccesses.hpp"

#include <LinearRegionMapImplementation.hpp>

#include <cassert>
#include <mutex>
#include <set>


CommutativeScoreboard::lock_t CommutativeScoreboard::_lock;
CommutativeScoreboard::map_t CommutativeScoreboard::_map;


bool CommutativeScoreboard::acquireEntry(CommutativeScoreboard::entry_t &entry)
{
	if (!entry._available) {
		return false;
	}

	entry._available = false;
	return true;
}


bool CommutativeScoreboard::addAndEvaluateTask(Task *task, CPUDependencyData &hpDependencyData)
{
	assert(task != nullptr);
	assert(hpDependencyData._acquiredCommutativeScoreboardEntries.empty());

	TaskDataAccesses &accessStructures = task->getDataAccesses();
	assert(accessStructures._totalCommutativeBytes != 0UL);

	bool successful = true;
	std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);
	accessStructures._accesses.processAll(
		[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
			DataAccess *dataAccess = &(*position);
			assert(dataAccess != nullptr);

			if ((dataAccess->getType() != COMMUTATIVE_ACCESS_TYPE) || dataAccess->isWeak()) {
				return true;
			}

			DataAccessRegion const &region = dataAccess->getAccessRegion();
			_map.processIntersectingAndMissing(
				region,
				[&](map_t::iterator mapPosition) -> bool {
					if (!mapPosition->getAccessRegion().fullyContainedIn(region)) {
						mapPosition = _map.fragmentByIntersection(mapPosition, region, /* removeIntersection */ false);
					}

					entry_t &entry = *mapPosition;
					entry._participants.insert(task);

					if (successful) {
						bool acquired = acquireEntry(entry);
						if (acquired) {
							assert(entry._location != nullptr);

							// Pick up the location from the scoreboard: since commutative tasks
							// are executed out of order, the location cannot be passed among
							// commutative tasks through the dependency system. Also, clear the
							// write ID because if the location changes between two commutative
							// accesses, then the data has been modified, so WriteID will never help.
							dataAccess->setLocation(entry._location);
							dataAccess->setNewWriteID();
							hpDependencyData._acquiredCommutativeScoreboardEntries.push_back(&entry);
						} else {
							successful = false;
						}
					}

					return true;
				},
				[&](DataAccessRegion const &missingRegion) -> bool {
					// The location is only missing in the scoreboard for the first commutative task.
					// Commutative tasks get the location twice: once when they become commutative satisfied,
					// when all commutative tasks get the initial location, and once again when they are
					// scheduled, with the current location from the previous commutative task. Here the
					// first task picks up the initial location.
					map_t::iterator mapPosition = _map.emplace(missingRegion);
					entry_t &entry = *mapPosition;
					entry._location = dataAccess->getLocation();
					entry._participants.insert(task);

					if (successful) {
						bool acquired = acquireEntry(entry);
						if (acquired) {
							hpDependencyData._acquiredCommutativeScoreboardEntries.push_back(&entry);
						} else {
							successful = false;
						}
					}

					return true;
				}
			);

			return true;
		}
	);

	if (!successful) {
		for (entry_t *entry : hpDependencyData._acquiredCommutativeScoreboardEntries) {
			assert(entry != nullptr);
			assert(!entry->_available);

			entry->_available = true;
		}
	}

	hpDependencyData._acquiredCommutativeScoreboardEntries.clear();

	return successful;
}


void CommutativeScoreboard::evaluateCompetingTask(
	Task *task,
	CPUDependencyData &hpDependencyData,
	CommutativeScoreboard::candidate_set_t &candidates
) {
	assert(task != nullptr);
	assert(hpDependencyData._acquiredCommutativeScoreboardEntries.empty());

	TaskDataAccesses &accessStructures = task->getDataAccesses();
	assert(accessStructures._totalCommutativeBytes != 0UL);

	bool successful = true;
	std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);
	accessStructures._accesses.processAll(
		[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
			DataAccess *dataAccess = &(*position);
			assert(dataAccess != nullptr);

			if ((dataAccess->getType() != COMMUTATIVE_ACCESS_TYPE) || dataAccess->isWeak()){
				return true;
			}

			DataAccessRegion const &region = dataAccess->getAccessRegion();
			_map.processIntersecting(
				region,
				[&](map_t::iterator mapPosition) -> bool {
					if (!mapPosition->getAccessRegion().fullyContainedIn(region)) {
						mapPosition = _map.fragmentByIntersection(mapPosition, region, /* removeIntersection */ false);
					}

					entry_t &entry = *mapPosition;
					entry._participants.insert(task);

					if (successful) {
						successful = acquireEntry(entry);
					}
					if (successful) {
						// Pick up the location from the scoreboard
						dataAccess->setLocation(entry._location);
						dataAccess->setNewWriteID();
						hpDependencyData._acquiredCommutativeScoreboardEntries.push_back(&entry);
					}

					// If this task cannot get the entry, it is because it is already reserved.
					// Therefore, none of the participants will either

					// NOTE: we do not let the current task be removed since
					// processReleasedCommutativeRegions has an iterator pointing to it.

					if (!successful && (/* see note */ candidates.size() > 1)) {
						for (Task *discardedCandidate : entry._participants) {
							assert(discardedCandidate != nullptr);

							if (discardedCandidate != task) {
								candidates.erase(discardedCandidate);
							}
						}
					}

					return true;
				}
			);

			return true;
		}
	);

	if (!successful) {
		for (entry_t *entry : hpDependencyData._acquiredCommutativeScoreboardEntries) {
			assert(entry != nullptr);
			assert(!entry->_available);

			entry->_available = true;
		}
	} else {
		// The task acquired all the commutative entries it required
		hpDependencyData._satisfiedOriginators.push_back(task);
	}

	hpDependencyData._acquiredCommutativeScoreboardEntries.clear();
}


void CommutativeScoreboard::processReleasedCommutativeRegions(CPUDependencyData &hpDependencyData)
{
	candidate_set_t candidates;

	// Mark the matching entries as available and collect a list of candidates
	for (CPUDependencyData::TaskAndRegion const &taskAndRegion : hpDependencyData._releasedCommutativeRegions) {
		_map.processIntersecting(
			taskAndRegion._region,
			[&](map_t::iterator mapPosition) -> bool {
				if (!mapPosition->getAccessRegion().fullyContainedIn(taskAndRegion._region)) {
					mapPosition = _map.fragmentByIntersection(mapPosition, taskAndRegion._region, /* removeIntersection */ false);
				}

				entry_t &entry = *mapPosition;

				__attribute__((unused)) size_t removedCount = entry._participants.erase(taskAndRegion._task);

				// Not sure why this assertion fails sometimes
				// assert(removedCount == 1);

				assert(!entry._available);
				if (!entry._participants.empty()) {
					for (Task *participant : entry._participants) {
						candidates.insert(participant);
					}
				}

				// Update the location in the scoreboard after this task so that it can be used
				// by the next commutative task. Note: even if there are no candidate tasks we
				// cannot delete the scoreboard entry yet. There may be more commutative tasks,
				// earlier in the sequential order, that are not ready yet because of other
				// dependencies. These tasks will have to take the location from the scoreboard
				// not the dependency system. We explicitly delete the scoreboard entries in
				// CommutativeScoreboard::endCommutative.
				entry._location = taskAndRegion._location;
				entry._available = true;

				return true;
			}
		);

	}

	// Evaluate the candidates
	for (Task *task : candidates) {
		evaluateCompetingTask(task, hpDependencyData, candidates);
	}

	candidates.clear();
	hpDependencyData._releasedCommutativeRegions.clear();
}

void CommutativeScoreboard::endCommutative(const DataAccessRegion region)
{
	// No more commutative tasks for this region. This is called when a non-commutative access
	// becomes satisfied, following one or more commutative tasks. This removes the scoreboard
	// entries so that the next commutative tasks on the same region use the correct location,
	// not the stale one that would be in the scoreboard.
	_map.processIntersecting(
		region,
		[&](map_t::iterator mapPosition) -> bool {
				if (!mapPosition->getAccessRegion().fullyContainedIn(region)) {
					mapPosition = _map.fragmentByIntersection(mapPosition, region, /* removeIntersection */ false);
				}

				_map.erase(mapPosition);
				return true;
		});
}

