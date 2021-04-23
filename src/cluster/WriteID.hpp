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

// "Forgetful" dictionary, intended to store the write IDs that are available
// locally together with the region that the write ID corresponds to.
//
// The operations are:
//
//    emplace(key,val): Add a non-zero key with its value to the set. It is
//    assumed that the keys are "randomized", so the key modulo the cache size
//    is a good hash. If there is a hash collision, then the old (key,value) is
//    lost.
//
//    remove(key): Remove a non-zero key by setting its cache entry to 0.
//
//    get(key): Return a pointer to the value for a given key, or nullptr
//              if the key is not present
template<class K, class V>
class CacheSet
{
private:
	struct keypair {
		K key;
		V value;
	};

	static constexpr int cacheSize = 256;
	std::vector<keypair> _buffer;

public:
	CacheSet() : _buffer(cacheSize)
	{
	}

	inline static size_t hash(const K key)
	{
		return key % cacheSize;
	}

	// Put non-zero val in the cache set
	inline void emplace(const K key, const V &value)
	{
		if (key) {
			_buffer.at(hash(key)) = {key, value};
		}
	}

	// Remove val from the cache set
	inline void remove(const K key)
	{
		if (key) {
			_buffer.at(hash(key)).key = 0;
		}
	}

	inline const V *get(const K key) const
	{
		if (key) {
			const size_t hashkey = hash(key);
			const keypair &pair = _buffer.at(hashkey);

			if (pair.key == key) {
				return &pair.value;
			}
		}
		return nullptr;
	}
};

typedef size_t HashID;
typedef size_t WriteID;

class WriteIDManager
{
private:
	static constexpr int logMaxNodes = 8;
	static WriteIDManager *_singleton;

	/* Counter */
	std::atomic<WriteID> _counter;
	CacheSet<HashID, DataAccessRegion> _localWriteIDs;

	static HashID hash(WriteID id, const DataAccessRegion &region)
	{
		size_t addr = (size_t)region.getStartAddress();
		size_t size = (size_t)region.getSize();
		return id + addr * 1234567 + size * 7654321;
	}

public:

	WriteIDManager(WriteID initCounter) : _localWriteIDs()
	{
		_counter.store(initCounter);
	}

	static void initialize(int nodeIndex, __attribute__((unused)) int clusterSize)
	{
		// The probability of collision is too high if a write ID has less than 64 bits
		static_assert(sizeof(WriteID) >= 8, "WriteID size is wrong.");
		assert(clusterSize < (1 << logMaxNodes));
		assert(_singleton == nullptr);

		WriteID initCounter = ((size_t)nodeIndex) << (64 - logMaxNodes);

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
			_singleton->_localWriteIDs.emplace(hash(id, region), region);
		}
	}

	// Register a write ID as not being present locally
	// (this will require reclaiming memory which is not
	// implemented yet).
	static void unregisterWriteIDnotLocal(WriteID id, const DataAccessRegion &region)
	{
		assert(_singleton != nullptr);
		_singleton->_localWriteIDs.remove(hash(id,region));
	}

	// Check whether a write ID is definitely present locally.
	static bool checkWriteIDLocal(WriteID id, const DataAccessRegion &region)
	{
		if (id) {
			assert(_singleton != nullptr);
			const DataAccessRegion *regionLocal = _singleton->_localWriteIDs.get(hash(id, region));
			return (regionLocal != nullptr && *regionLocal == region);
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
