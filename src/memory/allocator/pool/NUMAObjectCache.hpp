/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef __NUMA_OBJECT_CACHE_HPP__
#define __NUMA_OBJECT_CACHE_HPP__

#include "lowlevel/PaddedSpinLock.hpp"

#include <vector>
#include <deque>
#include <mutex>

template <typename T>
class NUMAObjectCache {

	typedef std::deque<T *> pool_t;
	typedef struct {
		PaddedSpinLock<> _lock;
		pool_t _pool;
	} NUMApool_t;

	std::vector<NUMApool_t> _NUMAPools;

public:
	NUMAObjectCache(size_t NUMANodeCount)
		: _NUMAPools(NUMANodeCount + 1)
	{
	}

	~NUMAObjectCache()
	{
	}

	/** This is called from a CPUNUMAObjectCache to fill up its pool of
	 *  objects.
	 *
	 * The NUMAObjectCache will try to get objects from the NUMA-specific
	 * pool. The method returns how many objects it managed to ultimately
	 * allocate
	 */
	inline int fillCPUPool(size_t numaId, std::deque<T *> &pool, size_t requestedObjects)
	{
		assert(numaId < _NUMAPools.size());
		assert(requestedObjects > 0);

		std::lock_guard<PaddedSpinLock<>> lock(_NUMAPools[numaId]._lock);
		pool_t &numaPool = _NUMAPools[numaId]._pool;

		const size_t poolSize = numaPool.size();
		if (poolSize == 0) {
			return 0;
		}

		const size_t nrObjects = std::min(requestedObjects, poolSize);
		std::move(numaPool.begin(), numaPool.begin() + nrObjects, std::front_inserter(pool));
		numaPool.erase(numaPool.begin(), numaPool.begin() + nrObjects);

		return nrObjects;
	}

	/** Method to return objects to a NUMA pool from a CPUNUMAObjectCache.
	 *
	 * This is typically called from a CPUNUMAObjectCache in order to return
	 * objects related with a different NUMA node than the one it belongs.
	 */
	void returnObjects(size_t numaId, std::deque<T *> &pool, size_t start = 0)
	{
		assert(numaId < _NUMAPools.size());

		std::lock_guard<PaddedSpinLock<>> lock(_NUMAPools[numaId]._lock);
		pool_t &numaPool = _NUMAPools[numaId]._pool;
		std::move(pool.begin() + start, pool.end(), std::back_inserter(numaPool));
		pool.erase(pool.begin() + start, pool.end());
	}
};

#endif /* __NUMA_OBJECT_CACHE_HPP__ */
