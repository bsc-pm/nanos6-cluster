/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef __CPU_OBJECT_CACHE_HPP__
#define __CPU_OBJECT_CACHE_HPP__

#include "Poison.hpp"
#include "lowlevel/SpinLock.hpp"
#include <deque>

#include <NUMAObjectCache.hpp>
#include <VirtualMemoryManagement.hpp>
#include <MemoryAllocator.hpp>

template <typename T>
class CPUObjectCache {
	NUMAObjectCache<T> *_NUMAObjectCache;
	const size_t _NUMANodeId;
	const size_t _numaNodeCount;

	size_t _allocationSize;

	typedef std::deque<T *> pool_t;

	/** Pools of available objects in the cache.
	 *
	 * We have one such pool per NUMA node, plus one extra for non-NUMA
	 * allocations, e.g. for ExternalThreads. When allocating an object
	 * the local pool will be used. When deleting an object it will be
	 * placed in the pool of the NUMA node in which the underlying memory
	 * belongs to. When the pool of a NUMA node other than ours reaches a
	 * certain limit, the objects of that pool will be returned to the
	 * cache of that NUMA node.
	 *
	 * These pools are not thread safe, i.e. they are meant to be accessed
	 * only by the thread that runs on the current CPU. */
	std::deque<pool_t> _available;

	CPUObjectCache () =  delete;

public:
	CPUObjectCache(NUMAObjectCache<T> *pool, size_t numaId, size_t numaNodeCount)
		: _NUMAObjectCache(pool), _NUMANodeId(numaId),
		_numaNodeCount(numaNodeCount), _allocationSize(1)
	{
		assert(numaId <= numaNodeCount);
		_available.resize(numaNodeCount + 1);
	}

	~CPUObjectCache()
	{
		_available.clear();
	}

	//! Allocate an object from the current CPU memory pool
	template <typename... TS>
	T *newObject(TS &&... args)
	{
		assert(_available.size() >= 1);
		assert(_NUMANodeId < _available.size());

		pool_t &local = _available.at(_NUMANodeId);

		if (local.empty()) {
			//! Try to recycle from NUMA pool
			const size_t allocated =
				_NUMAObjectCache->fillCPUPool(_NUMANodeId, local, 2 * _allocationSize);

			//! If NUMA pool did not have objects allocate new memory
			if (allocated == 0) {
				// Prevent overflow
				assert(_allocationSize < 2 * _allocationSize);

				_allocationSize *= 2;

				// Allocate from the per-CPU pool because:
				// (1) performance-critical and need to avoid taking a lock, and
				// (2) the CPUObjectCache redistributes free memory among CPUs via NUMAObjectCache
				T *ptr = (T *) MemoryAllocator::alloc(_allocationSize * sizeof(T), /* useCPUPool */ true);
				assert(ptr != nullptr);

				AddressSanitizer::poisonMemoryRegion(ptr, _allocationSize * sizeof(T));

				for (size_t i = 0; i < _allocationSize; ++i) {
					local.push_back(&ptr[i]);
				}
			}
		}

		T *ret = local.front();
		assert(ret != nullptr);
		local.pop_front();

		AddressSanitizer::unpoisonMemoryRegion(ret, sizeof(T));
		new (ret) T(std::forward<TS>(args)...);
		return ret;
	}

	//! Deallocate an object
	void deleteObject(T *ptr)
	{
		const size_t nodeId = VirtualMemoryManagement::findNUMA((void *)ptr);
		assert (nodeId < _available.size());
		ptr->~T();

		AddressSanitizer::poisonMemoryRegion(ptr, sizeof(T));
		_available[nodeId].push_front(ptr);

		// Return free objects to NUMA object cache
		if (nodeId != _NUMANodeId) {
			if (_available[nodeId].size() >= 64) {
				_NUMAObjectCache->returnObjects(nodeId, _available[nodeId]);
			}
		} else if (_available[nodeId].size() >= _allocationSize + 64*2) {
			// Return surplus free objects in local pool, keeping
			// _allocationSize+64 to reduce number of doublings of _allocationSize.
			_NUMAObjectCache->returnObjects(nodeId, _available[nodeId], 64);
		}
	}
};

#endif /* __CPU_OBJECT_CACHE_HPP__ */

