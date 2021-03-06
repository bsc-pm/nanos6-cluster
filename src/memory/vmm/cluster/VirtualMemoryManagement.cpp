/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include <string>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sys/mman.h>

#include "VirtualMemoryManagement.hpp"
#include "cluster/ClusterManager.hpp"
#include "cluster/messages/MessageId.hpp"
#include "hardware/HardwareInfo.hpp"
#include "hardware/cluster/ClusterNode.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "memory/vmm/VirtualMemoryAllocation.hpp"
#include "memory/vmm/VirtualMemoryArea.hpp"
#include "support/config/ConfigVariable.hpp"
#include "system/RuntimeInfo.hpp"

#include <DataAccessRegion.hpp>
#include <Directory.hpp>

std::vector<VirtualMemoryAllocation *> VirtualMemoryManagement::_allocations;
std::vector<VirtualMemoryArea *> VirtualMemoryManagement::_localNUMAVMA;
VirtualMemoryArea *VirtualMemoryManagement::_genericVMA;


//! \brief Returns a vector with all the mappings of the process
//!
//! It parses /proc/self/maps to find out all the current mappings of the
//! process.
//!
//! \returns a vector of DataAccessRegion objects describing the mappings
static std::vector<DataAccessRegion> findMappedRegions()
{
	std::vector<DataAccessRegion> maps;
	const char *mappingsFile = "/proc/self/maps";
	size_t len = 0;
	char *line = NULL;

	FILE *fp = fopen(mappingsFile, "r");
	assert(fp != NULL);

	ssize_t ret = getline(&line, &len, fp);
	FatalErrorHandler::failIf(ret == -1, "Could not find virtual memory mappings");

	while (ret != -1) {
		// First, we take the range which appears first to the line
		// separated by a space with everything that follows
		char *token = strtok(line, " ");
		assert(token != NULL);

		// Then we need to split the range which is two hexadecimals
		// separated by a '-'
		token = strtok(token, "-");
		void *startAddress = (void *)strtoll(token, NULL, 16);
		token = strtok(NULL, "-");
		void *endAddress = (void *)strtoll(token, NULL, 16);

		// The lower-end of the canonical virtual addresses finish
		// at the 2^47 limit. The upper-end of the canonical addresses
		// are normally used by the linux kernel. So we don't want to
		// look there.
		if ((size_t)endAddress >= (1UL << 47)) {
			break;
		}

		maps.emplace_back(startAddress, endAddress);

		// Read next line
		ret = getline(&line, &len, fp);
	}

	fclose(fp);

	return maps;
}

//! \brief Finds an memory region to map the Nanos6 region
//!
//! This finds the biggest common gap (region not currently mapped in the
//! virtual address space) of all Nanos6 instances and returns it.
//!
//! \returns an available memory region to map Nanos6 memory, or an empty region
//!          if none available
static DataAccessRegion findSuitableMemoryRegion()
{
	std::vector<DataAccessRegion> maps = findMappedRegions();
	const size_t length = maps.size();
	DataAccessRegion gap;

	// Find the biggest gap locally
	for (size_t i = 1; i < length; ++i) {
		void *previousEnd = maps[i - 1].getEndAddress();
		void *nextStart = maps[i].getStartAddress();

		if (previousEnd >= nextStart) {
			continue;
		}

		DataAccessRegion region(previousEnd, nextStart);
		if (region.getSize() > gap.getSize()) {
			gap = region;
		}
	}

	// If not in cluster mode, we are done here
	if (!ClusterManager::inClusterMode()) {
		return gap;
	}

	int messageId = MessageId::nextMessageId(DMALLOC);
	if (ClusterManager::isMasterNode()) {
		// Master node gathers all the gaps from all other nodes and
		// calculates the intersection of all those.
		DataAccessRegion remoteGap;
		DataAccessRegion buffer(&remoteGap, sizeof(remoteGap));

		std::vector<ClusterNode *> const &nodes = ClusterManager::getClusterNodes();

		for (ClusterNode *remote : nodes) {
			if (remote == ClusterManager::getCurrentClusterNode()) {
				continue;
			}

			MemoryPlace *memoryNode = remote->getMemoryNode();
			// do not instrument as instrumentation subsystem not initialized yet
			ClusterManager::fetchDataRaw(buffer, memoryNode, messageId, /* block */ true, /* instrument */ false);

			gap = gap.intersect(remoteGap);
		}

		// Finally, it send the common gap to all other nodes.
		remoteGap = gap;
		for (ClusterNode *remote : nodes) {
			if (remote == ClusterManager::getCurrentClusterNode()) {
				continue;
			}

			MemoryPlace *memoryNode = remote->getMemoryNode();
			// do not instrument as instrumentation subsystem not initialized yet
			ClusterManager::sendDataRaw(buffer, memoryNode, messageId, /* block */ true, /* instrument */ false);
		}
	} else {
		DataAccessRegion buffer(&gap, sizeof(gap));
		ClusterNode *master = ClusterManager::getMasterNode();
		MemoryPlace *masterMemory = master->getMemoryNode();

		// First send my local gap to master node
		ClusterManager::sendDataRaw(buffer, masterMemory, messageId, /* block */ true, /* instrument */ false);

		// Then receive the intersection of all gaps
		ClusterManager::fetchDataRaw(buffer, masterMemory, messageId, /* block */ true, /* instrument */ false);
	}

	return gap;
}

void VirtualMemoryManagement::initialize()
{
	// The cluster.distributed_memory variable determines the total address space to be
	// used for distributed allocations across the cluster The efault value is 2GB
	ConfigVariable<StringifiedMemorySize> distribSizeEnv("cluster.distributed_memory");
	size_t distribSize = distribSizeEnv.getValue();
	assert(distribSize > 0);
	distribSize = ROUND_UP(distribSize, HardwareInfo::getPageSize());

	// The cluster.local_memory variable determines the size of the local address space per cluster
	// node.  If the value is not set in the config file then the runtime will use the minimum
	// between 2GB and the 5% of the total physical memory of the machine if not set.
	ConfigVariable<StringifiedMemorySize> localSizeEnv("cluster.local_memory");
	size_t localSize = localSizeEnv.getValue();

	// If the config value is zero it means it is not set. As zero local virtual memory is not a
	// useful value any way.
	if (localSize == 0) {
		const size_t totalMemory = HardwareInfo::getPhysicalMemorySize();
		localSize = std::min(2UL << 30, totalMemory / 20);
	}
	assert(localSize > 0);
	localSize = ROUND_UP(localSize, HardwareInfo::getPageSize());

	ConfigVariable<uint64_t> startAddress("cluster.va_start");
	void *address = (void *) startAddress.getValue();
	size_t size = distribSize + localSize * ClusterManager::clusterSize();

	if (address == nullptr) {
		DataAccessRegion gap = findSuitableMemoryRegion();
		address = gap.getStartAddress();
		FatalErrorHandler::failIf(gap.getSize() < size, "Cannot allocate virtual memory region");
	}

	assert(_allocations.empty());
	_allocations.resize(1);
	_allocations[0] = new VirtualMemoryAllocation(address, size);

	setupMemoryLayout(address, distribSize, localSize);

	RuntimeInfo::addEntry("distributed_memory_size", "Size of distributed memory", distribSize);
	RuntimeInfo::addEntry("local_memorysize", "Size of local memory per node", localSize);
	RuntimeInfo::addEntry("va_start", "Virtual address space start", (unsigned long)address);
}

void VirtualMemoryManagement::shutdown()
{
	for (auto &vma : _localNUMAVMA) {
		DataAccessRegion numaRegion(vma->getAddress(), vma->getSize());
		Directory::erase(numaRegion);
		delete vma;
	}

	delete _genericVMA;

	for (auto &alloc : _allocations) {
		delete alloc;
	}

	_localNUMAVMA.clear();
	_allocations.clear();
}

void VirtualMemoryManagement::setupMemoryLayout(void *address, size_t distribSize, size_t localSize)
{
	ClusterNode *current = ClusterManager::getCurrentClusterNode();
	const int nodeIndex = current->getIndex();
	const int clusterSize = ClusterManager::clusterSize();

	void *distribAddress = (void *)((char *)address + clusterSize * localSize);
	_genericVMA = new VirtualMemoryArea(distribAddress, distribSize);
	void *localAddress = (void *)((char *)address + nodeIndex * localSize);

	// Register local addresses with the Directory
	for (int i = 0; i < clusterSize; ++i) {
		if (i == nodeIndex) {
			continue;
		}

		void *ptr = (void *)((char *)address + i * localSize);
		DataAccessRegion localRegion(ptr, localSize);
		Directory::insert(localRegion, ClusterManager::getMemoryNode(i));
	}

	// We have one VMA per NUMA node. At the moment we divide the local
	// address space equally among these areas.
	assert(_localNUMAVMA.empty());
	const size_t numaNodeCount =
		HardwareInfo::getMemoryPlaceCount(nanos6_device_t::nanos6_host_device);
	_localNUMAVMA.resize(numaNodeCount);

	// Divide the address space between the NUMA nodes and the
	// making sure that all areas have a size that is multiple
	// of PAGE_SIZE
	const size_t pageSize = HardwareInfo::getPageSize();
	const size_t localPages = localSize / pageSize;
	const size_t pagesPerNUMA = localPages / numaNodeCount;
	const size_t sizePerNUMA = pagesPerNUMA * pageSize;

	size_t extraPages = localPages % numaNodeCount;
	char *ptr = (char *)localAddress;
	for (size_t i = 0; i < numaNodeCount; ++i) {
		size_t numaSize = sizePerNUMA;
		if (extraPages > 0) {
			numaSize += pageSize;
			extraPages--;
		}
		_localNUMAVMA[i] = new VirtualMemoryArea(ptr, numaSize);

		// Register the region with the Directory
		DataAccessRegion numaRegion(ptr, numaSize);
		Directory::insert(numaRegion, HardwareInfo::getMemoryPlace(nanos6_host_device, i));

		ptr += numaSize;
	}
}
