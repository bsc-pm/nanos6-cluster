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

#include "DataAccessRegion.hpp"

// Identify redundant data fetches using a write ID, which is shared among the
// cluster nodes. Each new data output or inout is allocated a new write ID.
// The write ID is propagated to the next dependent read only if the read is
// for exactly the same region, otherwise the region has an invalid write ID of
// zero. These functions check that both the write ID and the region match.
//
// False negatives (claiming no local copy of a write ID when there is one) are
// safe. This causes an unnecessary (but correct) data fetch.
//
// False positives (claiming there is a local copy when there isn't one) will
// cause incorrect execution so need to be avoided. This can only happen if two
// different writes to the same region create the same write ID AND the old
// write ID is present on a node that accesses this data. Since write IDs are
// 64-bit and a cache contains for example 256 recent write IDs, there is a
// probability of 1 in 2^48 that any call to 'checkWriteIDLocal' will return a
// false positive.  Assuming that every task accesses this region, this allows
// 2^41 tasks to be executed before there is (almost) a one-in-100 chance of a
// false positive.


// 64-bit Write ID.
typedef size_t WriteID;

typedef size_t HashID;
typedef size_t WriteID;

class WriteIDManager
{
private:
	static constexpr int logMaxNodes = 8;
	static WriteIDManager *_singleton;

	/* Counter */
	std::atomic<WriteID> _counter;
	struct keypair {
		HashID key;
		DataAccessRegion value;
	};

	static constexpr int cacheSize = 8192;
	std::vector<keypair> _localWriteIDs;

	static HashID hash(WriteID id, const DataAccessRegion &region)
	{
		size_t addr = (size_t)region.getStartAddress();
		size_t size = (size_t)region.getSize();
		// Based on https://xorshift.di.unimi.it/splitmix64.c
		uint64_t z =  id + addr * 1234567 + size * 7654321;
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
		return z ^ (z >> 31);
	}

public:

	WriteIDManager(WriteID initCounter) : _localWriteIDs(cacheSize)
	{
		_counter.store(initCounter);
	}

	static void initialize(int nodeIndex, __attribute__((unused)) int clusterSize)
	{
		// The probability of collision is too high if a write ID has less than 64 bits
		static_assert(sizeof(WriteID) >= 8, "WriteID size is wrong.");
		assert(clusterSize < (1 << logMaxNodes));
		assert(_singleton == nullptr);

		WriteID initCounter = 1 + (((size_t)nodeIndex) << (64 - logMaxNodes));

		_singleton = new WriteIDManager(initCounter);
		//std::cout << "construct WriteIDManager " << nodeIndex << " with counter: " << initCounter << "\n";
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
			HashID key = hash(id, region);
			_singleton->_localWriteIDs.at(key % cacheSize) = {key, region};
		}
	}

	// Register a write ID as not being present locally
	// (this will require reclaiming memory which is not
	// implemented yet).
	static void unregisterWriteIDnotLocal(WriteID id, const DataAccessRegion &region)
	{
		assert(_singleton != nullptr);
		HashID key = hash(id, region);
		_singleton->_localWriteIDs.at(key % cacheSize).key = 0;
	}

	// Check whether a write ID is definitely present locally.
	static bool checkWriteIDLocal(WriteID id, const DataAccessRegion &region)
	{
		if (id) {
			assert(_singleton != nullptr);
			HashID key = hash(id, region);
			if (key) {
				const keypair &pair = _singleton->_localWriteIDs.at(key % cacheSize);
				if (pair.key == key && pair.value == region) {
					return true;
				}
			}
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
