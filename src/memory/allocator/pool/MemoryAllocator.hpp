/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef MEMORY_ALLOCATOR_HPP
#define MEMORY_ALLOCATOR_HPP

#include <map>
#include <memory>
#include <vector>

#include "lowlevel/SpinLock.hpp"

class MemoryPool;
class MemoryPoolGlobal;
class Task;
struct DataAccess;

class MemoryAllocator {
private:
	// This variable is here to keep uniformity with the other Allocators because in this one we
	// already have the _singleton for exactly the same purpose. But if we remove the _singleton in
	// the future, we will still have this one.
	static MemoryAllocator *_singleton;

	typedef std::map<size_t, MemoryPool *> size_to_pool_t;

	std::vector<MemoryPoolGlobal *> _globalMemoryPool;
	std::vector<size_to_pool_t> _localMemoryPool;

	SpinLock _externalMemoryPoolLock;
	size_to_pool_t _externalMemoryPool;

	bool getPool(size_t size, bool useCPUPool, MemoryPool *&pool);

	MemoryAllocator(size_t numaNodeCount, size_t cpuCount);
	~MemoryAllocator();

	// Cache this value to avoid shutdown collisions with HardwareInfo
	// We assume this vale not change during the execution.
	const size_t _cacheLineSize;

public:
	static void initialize();
	static void shutdown();

	// Only allocate and free from the CPU pool if:
	// (1) performance-critical and need to avoid taking a lock, and
	// (2) either alloc and free happen on the same CPU or there is
	//     a mechanism to redistribute free memory among cores
	static void *alloc(size_t size, bool useCPUPool = false);
	static void free(void *chunk, size_t size, bool useCPUPool = false);

	static constexpr bool hasUsageStatistics()
	{
		return false;
	}

	static inline size_t getMemoryUsage()
	{
		return 0;
	}

	/* Simplifications for using "new" and "delete" with the allocator */
	template <typename T, typename... Args>
	static T *newObject(Args &&... args)
	{
		void *ptr = MemoryAllocator::alloc(sizeof(T));
		assert(ptr != nullptr);
		new (ptr) T(std::forward<Args>(args)...);
		return (T*)ptr;
	}

	template <typename T>
	static void deleteObject(T *ptr)
	{
		assert(ptr != nullptr);
		ptr->~T();
		MemoryAllocator::free(ptr, sizeof(T));
	}


	static inline bool isInitialized()
	{
		return (_singleton != nullptr);
	}
};

template<typename T>
using TemplateAllocator = std::allocator<T>;

#endif // MEMORY_ALLOCATOR_HPP
