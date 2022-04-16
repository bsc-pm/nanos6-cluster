/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef WRITEID_HPP
#define WRITEID_HPP

#include <cstddef>
#include <cstdint>
#include <utility>
#include <atomic>
#include <vector>
#include <iostream>

#include "lowlevel/RWSpinLock.hpp"
#include "DataAccessRegion.hpp"
#include "LinearRegionMap.hpp"
#include "LinearRegionMapImplementation.hpp"

// Identify redundant data fetches using a globally unique write ID.  Each new
// data output or inout is allocated a new write ID.  The write ID remains the
// same for all dependent read accesses.
//
// We hold the tree of locally present regions and WriteIDs in a
// LinearRegionMap, which means that subaccesses of the original access are
// also identified as locally present.  Larger accesses, all of whose parts
// have been registered as present, are also identified as such. The API does
// not however, let us identify which parts (if not all) of a larger access are
// present.
//
// Accessing this tree requires taking a (reader/writer) lock, so we hash the
// WriteID and use one of a small number of trees. The number of trees is given
// by numMaps. This means that read and updates for the trees corresponding to
// different WriteIDs can be done concurrently. Note: Different values of
// numMaps does not change the behaviour of WriteID, but it may affect the
// locking overhead.
//
// Note: the old WriteID is not removed when the same region with a later
// WriteID is registered as present. But the dependency system already ensures
// that we only check the WriteID for the currently-valid WriteID.

// 64-bit Write ID.
typedef size_t WriteID;

typedef size_t HashID;
typedef size_t WriteID;

// Represent a region that is present locally with a given WriteID
struct WriteIDEntryRegion {
	DataAccessRegion _region;
	WriteID _writeID;

	DataAccessRegion const &getAccessRegion() const
	{
		return _region;
	}

	DataAccessRegion &getAccessRegion()
	{
		return _region;
	}

	WriteIDEntryRegion(DataAccessRegion region) : _region(region)
	{
	}
};

typedef LinearRegionMap<WriteIDEntryRegion> WriteIDLinearRegionMap;

struct WriteIDEntry {
	RWSpinLock _lock;
	WriteIDLinearRegionMap _regions;
};

class WriteIDManager
{
private:
	static constexpr int logMaxNodes = 8;
	static WriteIDManager *_singleton;

	/* Counter */
	std::atomic<WriteID> _counter;

	static constexpr int numMaps = 512;
	std::vector<WriteIDEntry> _localWriteIDs;

	static HashID hash(WriteID id)
	{
		// Based on https://xorshift.di.unimi.it/splitmix64.c
		uint64_t z =  id;
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
		return z ^ (z >> 31);
	}

	WriteIDManager() : _localWriteIDs(numMaps)
	{
	}

public:
	static void initialize(int nodeIndex, int clusterSize)
	{
		assert(_singleton == nullptr);
		_singleton = new WriteIDManager();
		WriteIDManager::reset(nodeIndex, clusterSize);
	}

	static void reset(int nodeIndex, __attribute__((unused)) int clusterSize)
	{
		// The probability of collision is too high if a write ID has less than 64 bits
		static_assert(sizeof(WriteID) >= 8, "WriteID size is wrong.");
		assert(_singleton != nullptr);
		assert(clusterSize < (1 << logMaxNodes));

		WriteID initCounter = 1 + (((size_t)nodeIndex) << (64 - logMaxNodes));
		_singleton->_counter.store(initCounter);
	}

	static void finalize()
	{
		assert(_singleton != nullptr);
		delete _singleton;
		_singleton = nullptr;
	}

	// Register a write ID as being present locally
	static void registerWriteIDasLocal(WriteID id, const DataAccessRegion &region)
	{
		if (id) {
			assert(_singleton != nullptr);

			// Find the right tree and take a writer lock
			int idx = hash(id) % numMaps;
			WriteIDEntry &entry = _singleton->_localWriteIDs.at(idx);
			entry._lock.writeLock();

			// Update the tree
			entry._regions.processIntersectingAndMissing(region,
				[&](__attribute__((unused)) WriteIDLinearRegionMap::iterator position) -> bool {
					// Region already in the map: update the writeID if it has changed
					if (position->_writeID != id) {
						if (!position->getAccessRegion().fullyContainedIn(region)) {
							position = entry._regions.fragmentByIntersection(position, region, /* removeIntersection */ false);
						}
						position->_writeID = id;
					}
					return true;
				},
				[&](DataAccessRegion const &missingRegion) -> bool {
					// Region not yet in the map: insert it with the given WriteID
					WriteIDLinearRegionMap::iterator position = entry._regions.emplace(missingRegion);
					position->_writeID = id;
					return true;
				}
			);

			entry._lock.writeUnlock();
		}
	}

	// Check whether the whole region for a write ID is definitely present locally.
	static bool checkWriteIDLocal(WriteID id, const DataAccessRegion &region)
	{
		if (id) {
			assert(_singleton != nullptr);

			// Find the right tree and take a reader lock
			int idx = hash(id) % numMaps;
			WriteIDEntry &entry = _singleton->_localWriteIDs.at(idx);
			size_t bytesFound = 0;
			entry._lock.readLock();

			// Add up all the bytes in this region that correspond to the correct WriteID
			entry._regions.processIntersecting(region,
				[&](WriteIDLinearRegionMap::iterator position) -> bool {
					if (position->_writeID == id) {
						const DataAccessRegion foundRegion = position->getAccessRegion();
						DataAccessRegion subregion = foundRegion.intersect(region);
						bytesFound += subregion.getSize();
					}
					return true;
				}
			);
			entry._lock.readUnlock();

			// Return true if all of the bytes of the region have been found
			assert(bytesFound <= region.getSize());
			return bytesFound == region.getSize();
		}
		return false;
	}

	static inline WriteID createWriteID()
	{
		assert(_singleton != nullptr);
		/* This happens for every access, so it should be fast */
		return _singleton->_counter.fetch_add(1);
	}
};

#endif // WRITEID_HPP
