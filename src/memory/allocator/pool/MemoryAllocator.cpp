/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2017 Barcelona Supercomputing Center (BSC)
*/

#include "executors/threads/CPU.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "hardware/HardwareInfo.hpp"
#include <VirtualMemoryManagement.hpp>

#include "MemoryAllocator.hpp"
#include "ObjectAllocator.hpp"

#include "MemoryPool.hpp"
#include "MemoryPoolGlobal.hpp"

#include "Poison.hpp"


MemoryAllocator *MemoryAllocator::_singleton = nullptr;

MemoryAllocator::MemoryAllocator(size_t numaNodeCount, size_t cpuCount) :
	_globalMemoryPool(numaNodeCount),
	_localMemoryPool(cpuCount),
	_externalMemoryPool(),
	_cacheLineSize(HardwareInfo::getCacheLineSize())
{
	assert(cpuCount > 0);

	for (size_t i = 0; i < numaNodeCount; ++i) {
		_globalMemoryPool[i] = new MemoryPoolGlobal(i);
	}
}

MemoryAllocator::~MemoryAllocator()
{
	for (auto &it : _externalMemoryPool) {
		delete it.second;
	}

	for (auto &it : _localMemoryPool) {
		for (auto &it_i : it) {
			delete it_i.second;
		}
	}

	for (auto &it : _globalMemoryPool) {
		delete it;
	}

	_localMemoryPool.clear();
	_globalMemoryPool.clear();
}

// Static functions start here

bool MemoryAllocator::getPool(size_t size, bool useCPUPool, MemoryPool *&pool)
{
	pool = nullptr;

	// Round to the nearest multiple of the cache line size
	const size_t cacheLineSize = _singleton->_cacheLineSize;
	const size_t roundedSize = (size + _cacheLineSize - 1) & ~(_cacheLineSize - 1);
	const size_t cacheLines = roundedSize / cacheLineSize;
	bool isExternal;

	assert (roundedSize > 0);
	if (useCPUPool) {
		WorkerThread *thread = WorkerThread::getCurrentWorkerThread();

		if (thread != nullptr) {
			CPU *currentCPU = thread->getComputePlace();

			if (currentCPU != nullptr) {
				const size_t cpuId = currentCPU->getIndex();
				assert(cpuId < _localMemoryPool.size());

				auto it = _localMemoryPool[cpuId].find(cacheLines);
				if (it == _localMemoryPool[cpuId].end()) {
					const size_t numaNodeId = currentCPU->getNumaNodeId();
					assert(numaNodeId < _globalMemoryPool.size());

					// No pool of this size locally
					pool = new MemoryPool(_globalMemoryPool[numaNodeId], roundedSize);
					_localMemoryPool[cpuId][cacheLines] = pool;
				} else {
					pool = it->second;
				}
			}
		}
	}

	if (pool == nullptr) {
		isExternal = true;
		std::lock_guard<SpinLock> guard(_singleton->_externalMemoryPoolLock);
		auto it = _externalMemoryPool.find(cacheLines);
		if (it == _externalMemoryPool.end()) {
			pool = new MemoryPool(_singleton->_globalMemoryPool[0], roundedSize);
			_externalMemoryPool[cacheLines] = pool;
		} else {
			pool = it->second;
		}
	} else {
		isExternal = false;
	}

	return isExternal;
}

void MemoryAllocator::initialize()
{
	assert(init == false);
	init = true;

	// This is a cached vale.
	const size_t numaNodeCount
		= HardwareInfo::getMemoryPlaceCount(nanos6_device_t::nanos6_host_device);
	const size_t cpuCount = CPUManager::getTotalCPUs();

	VirtualMemoryManagement::initialize();

	assert(_singleton == nullptr);
	_singleton = new MemoryAllocator(numaNodeCount, cpuCount);

	//! Initialize the Object caches
	ObjectAllocator<DataAccess>::initialize();
	ObjectAllocator<ReductionInfo>::initialize();
	ObjectAllocator<BottomMapEntry>::initialize();
}

void MemoryAllocator::shutdown()
{
	assert(init == true);
	//! Initialize the Object caches
	ObjectAllocator<BottomMapEntry>::shutdown();
	ObjectAllocator<ReductionInfo>::shutdown();
	ObjectAllocator<DataAccess>::shutdown();

	assert(_singleton != nullptr);
	delete _singleton;
	_singleton = nullptr;

	VirtualMemoryManagement::shutdown();

	init = false;
}


void *MemoryAllocator::alloc(size_t size, bool useCPUPool)
{
	assert(init == true);
	assert(_singleton != nullptr);
	MemoryPool *pool;
	bool isExternal = _singleton->getPool(size, useCPUPool, pool);

	assert(pool != nullptr);

	if (!isExternal) {
		return pool->getChunk();
	} else {
		std::lock_guard<SpinLock> guard(_singleton->_externalMemoryPoolLock);
		return pool->getChunk();
	}
}

void MemoryAllocator::free(void *chunk, size_t size, bool useCPUPool)
{
	assert(init == true);
	assert(_singleton != nullptr);
	MemoryPool *pool;
	bool isExternal = _singleton->getPool(size, useCPUPool, pool);

	assert(pool != nullptr);

	if (!isExternal) {
		pool->returnChunk(chunk);
	} else {
		std::lock_guard<SpinLock> guard(_singleton->_externalMemoryPoolLock);
		pool->returnChunk(chunk);
	}
}
