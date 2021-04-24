/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include <sys/mman.h>
#include <iostream>
#include <string>
#include <regex>

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

	const std::regex line_regex(
		"([[:xdigit:]]+)-([[:xdigit:]]+) [rwpx-]{4} [[:xdigit:]]+ [[:xdigit:]]{2}:[[:xdigit:]]{2} [0-9]+ +(.*)");

	std::ifstream mapfile("/proc/self/maps");

	std::string line;

	if (mapfile.is_open()) {
		while (getline(mapfile, line)) {

			std::smatch submatch;
			if (std::regex_match(line, submatch, line_regex)) {
				assert(submatch.ready());
				size_t startAddress = std::stoull(submatch[1].str(), NULL, 16);
				size_t endAddress = std::stoull(submatch[2].str(), NULL, 16);

				// The lower-end of the canonical virtual addresses finish
				// at the 2^47 limit. The upper-end of the canonical addresses
				// are normally used by the linux kernel. So we don't want to
				// look there.
				if (endAddress >= (1UL << 47)) {
					break;
				}

				// Add an extra padding to the end of the heap.
				// This avoids the nasty memory error we had long time ago.
				if (submatch[3].str() == "[heap]") {
					endAddress += HardwareInfo::getPhysicalMemorySize();
				}

				maps.emplace_back((void *)startAddress, (void *)endAddress);
			}
		}

		if (!mapfile.eof()) {
			// Check the fail only if not at eof.
			FatalErrorHandler::failIf(mapfile.fail(), "Could not read virtual memory mappings");
		}
		mapfile.close();
	} else {
		FatalErrorHandler::fail("Could not open memory mappings file");
	}

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
			ClusterManager::fetchDataRaw(buffer, memoryNode, 0, /* block */ true, /* instrument */ false);

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
			ClusterManager::sendDataRaw(buffer, memoryNode, 0, /* block */ true, /* instrument */ false);
		}
	} else {
		DataAccessRegion buffer(&gap, sizeof(gap));
		MemoryPlace *masterMemory = ClusterManager::getMasterNode()->getMemoryNode();

		// First send my local gap to master node
		ClusterManager::sendDataRaw(buffer, masterMemory, 0, /* block */ true, /* instrument */ false);

		// Then receive the intersection of all gaps
		ClusterManager::fetchDataRaw(buffer, masterMemory, 0, /* block */ true, /* instrument */ false);
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

	// The cluster.local_memory variable determines the size of the local address space
	// per cluster node. The default value is the minimum between 2GB and the 5% of the
	// total physical memory of the machine
	ConfigVariable<StringifiedMemorySize> localSizeEnv("cluster.local_memory");
	size_t localSize = localSizeEnv.getValue();

	// localSize == 0 when not set in any toml.
	if (localSize == 0) {
		FatalErrorHandler::warn("cluster.local_memory not from toml.");
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
	assert(distribSize > 0);
	assert(localSize > 0);
	const ClusterNode *current = ClusterManager::getCurrentClusterNode();
	const std::vector<ClusterNode *> &nodesList = ClusterManager::getClusterNodes();

	const void *distribAddress = (void *)((char *)address + localSize * nodesList.size());
	_genericVMA = new VirtualMemoryArea(distribAddress, distribSize);
	const char *localAddress = (char *)address + localSize * current->getIndex();

	// Register local addresses with the Directory
	for (ClusterNode *node : nodesList) {
		if (node == current) {
			continue;
		}

		void *ptr = (void *)((char *)address + localSize * node->getIndex());
		DataAccessRegion localRegion(ptr, localSize);
		Directory::insert(localRegion, node->getMemoryNode());
	}

	// We have one VMA per NUMA node. At the moment we divide the local
	// address space equally among these areas.
	assert(_localNUMAVMA.empty());
	const size_t numaNodeCount =
		HardwareInfo::getMemoryPlaceCount(nanos6_device_t::nanos6_host_device);
	assert(numaNodeCount > 0);
	_localNUMAVMA.resize(numaNodeCount);

	// Divide the address space between the NUMA nodes and the
	// making sure that all areas have a size that is multiple
	// of PAGE_SIZE
	const size_t pageSize = HardwareInfo::getPageSize();
	assert(pageSize > 0);
	const size_t localPages = localSize / pageSize;
	assert(localPages > 0);
	const size_t pagesPerNUMA = localPages / numaNodeCount;
	const size_t sizePerNUMA = pagesPerNUMA * pageSize;
	assert(sizePerNUMA > 0);
	assert(pagesPerNUMA > 0);

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
		const DataAccessRegion numaRegion(ptr, numaSize);
		Directory::insert(numaRegion, HardwareInfo::getMemoryPlace(nanos6_host_device, i));

		ptr += numaSize;
	}
}
