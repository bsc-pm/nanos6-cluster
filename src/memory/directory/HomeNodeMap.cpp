/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include <DataAccessRegion.hpp>

#include "HomeMapEntry.hpp"
#include "HomeNodeMap.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "hardware/places/MemoryPlace.hpp"

void HomeNodeMap::insert(
	DataAccessRegion const &region,
	MemoryPlace const *homeNode
) {
	lock.writeLock();

	processIntersectingAndMissing(
		region,
		[&] (HomeNodeMap::iterator pos) -> bool {
			HomeMapEntry *entry = &(*pos);

			FatalErrorHandler::failIf(
				true,
				"We do not support updating the home node of a region. (Region: ",
				entry->getAccessRegion(),
				")"
			);

			//! Just to avoid compilation warnings
			return true;
		},
		[&] (DataAccessRegion missingRegion) -> bool {
			HomeMapEntry *entry = new HomeMapEntry(missingRegion, homeNode);
			BaseType::insert(*entry);

			return true;
		}
	);
	lock.writeUnlock();
}

HomeNodeMap::HomeNodesArray *HomeNodeMap::find(DataAccessRegion const &region)
{
	lock.readLock();
	HomeNodesArray *ret = new HomeNodesArray();

	processIntersectingAndMissing(
		region,
		[&] (HomeNodeMap::iterator pos) -> bool {
			HomeMapEntry *entry = &(*pos);
			ret->push_back(entry);
			return true;
		},
		[&] (__attribute__((unused)) DataAccessRegion missingRegion) -> bool {
			return true;
		}
	);

	lock.readUnlock();
	return ret;
}

void HomeNodeMap::erase(DataAccessRegion const &region)
{
	lock.writeLock();
	processIntersectingAndMissing(
		region,
		[&] (HomeNodeMap::iterator pos) -> bool {
			HomeMapEntry *entry = &(*pos);
			BaseType::erase(entry);
			delete entry;
			return true;
		},
		[&] (DataAccessRegion missingRegion) -> bool {
			FatalErrorHandler::failIf(
				true,
				"Trying to erase an unknown home node mapping for region: ",
				missingRegion
			);

			//! Just to avoid compilation warnings
			return true;
		}
	);
	lock.writeUnlock();
}
