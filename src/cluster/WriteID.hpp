/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef WRITEID_HPP
#define WRITEID_HPP

#include <cstddef>
#include <cstdint>
#include <utility>
#include <random>

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
template<class K,class V, typename...Ts> class CacheSet
{
private:
	static const int cacheSize = 256;
	std::vector<K> keys;
	std::vector<V> values;

public:
	CacheSet() : keys(cacheSize), values(cacheSize)
	{
	}

	inline static size_t hash(const K key)
	{
		return key % cacheSize;
	}

	// Put non-zero val in the cache set
	inline void emplace(const K key, Ts&&... args)
	{
		if (key) {
			size_t hashkey = hash(key);
			keys[hashkey] = key;
			new (&values[hashkey]) V(std::forward<Ts>(args)...);
		}
	}

	// Remove val from the cache set
	inline void remove(const K key)
	{
		if (key) {
			keys[hash(key)] = 0;
		}
	}

	inline const V *get(const K key) const
	{
		if (!key) {
			return nullptr;
		}
		size_t hashkey = hash(key);
		if (keys[hashkey] == key) {
			return &values[hashkey];
		} else {
			return nullptr;
		}	
	}
};

typedef size_t HashID;
typedef size_t WriteID;

class WriteIDManager
{
private:
	static WriteIDManager *_singleton;
	static CacheSet<HashID, DataAccessRegion, void *, size_t> _localWriteIDs;

	/* Random number generator */
	std::random_device _rd;
	std::mt19937_64 _eng;

	static HashID hash(WriteID id, const DataAccessRegion &region)
	{
		size_t addr = (size_t)region.getStartAddress();
		size_t size = (size_t)region.getSize();
		return id + addr * 1234567 + size * 7654321;
	}

public:

	WriteIDManager() : _eng(_rd())
	{
	}

	static void initialize()
	{
		// The probability of collision is too high if a write ID has less than 64 bits
		assert(sizeof(WriteID) >= 8);

		_singleton = new WriteIDManager;
	}

	// Register a write ID as being present locally
	static void registerWriteIDasLocal(WriteID id, const DataAccessRegion &region)
	{
		if (id) { 
			_localWriteIDs.emplace(hash(id,region),
								   region.getStartAddress(),
								   region.getSize());
		}
	}

	// Register a write ID as not being present locally
	// (this will require reclaiming memory which is not
	// implemented yet).
	static void unregisterWriteIDnotLocal(WriteID id, const DataAccessRegion &region)
	{
		_localWriteIDs.remove(hash(id,region));
	}

	// Check whether a write ID is definitely present locally.
	static bool checkWriteIDLocal(WriteID id, const DataAccessRegion &region)
	{
		if (!id) {
			return false;
		}
		const DataAccessRegion *regionLocal = _localWriteIDs.get(hash(id, region));
		if (!regionLocal) {
			return false;
		}
		return *regionLocal == region;
	}

	static WriteID createRandomWriteID()
	{
		assert(_singleton);
		WriteID id =  _singleton->_eng();
		// std::cout << "Returned random write ID " << std::hex << id << std::dec << "\n";
		return id;
	}

};

#endif // WRITEID_HPP
