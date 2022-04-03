/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef __VIRTUAL_MEMORY_MANAGEMENT_HPP__
#define __VIRTUAL_MEMORY_MANAGEMENT_HPP__

#include "memory/vmm/VirtualMemoryArea.hpp"

#include <vector>

class VirtualMemoryManagement {
public:
	// Subclass allocation; only needed and used here
	class VirtualMemoryAllocation : public DataAccessRegion
	{
	public:
		VirtualMemoryAllocation(void *address, size_t size) : DataAccessRegion(address, size)
		{
			FatalErrorHandler::failIf(size == 0, "Virtual memory constructor receive a zero size.");

			/** For the moment we are using fixed memory protection and allocation flags, but in the
			 * future we could make those arguments fields of the class */
			const int prot = PROT_READ|PROT_WRITE;
			int flags = MAP_ANONYMOUS|MAP_PRIVATE|MAP_NORESERVE;

			if (address != nullptr) {
				flags |= MAP_FIXED;
			}
			void *ret = mmap(address, size, prot, flags, -1, 0);

			FatalErrorHandler::failIf(ret == MAP_FAILED,
				"mapping virtual address space failed. errno: ", errno);
			FatalErrorHandler::failIf(ret != address,
				"mapping virtual address space couldn't use address hint");
		}

		~VirtualMemoryAllocation()
		{
			const int ret = munmap(this->getStartAddress(), this->getSize());
			FatalErrorHandler::failIf(ret != 0, "Could not unmap memory allocation");
		}
	};

private:
	//! memory allocations from OS
	std::vector<VirtualMemoryManagement::VirtualMemoryAllocation *> _allocations;

	//! addresses for local NUMA allocations
	std::vector<VirtualMemoryArea *> _localNUMAVMA;

	//! addresses for generic allocations
	VirtualMemoryArea *_genericVMA;

	//! Setting up the memory layout
	void setupMemoryLayout(void *address, size_t distribSize, size_t localSize);

	//! private constructor, this is a singleton.
	VirtualMemoryManagement();

	~VirtualMemoryManagement();

	static VirtualMemoryManagement *_singleton;

public:

	static inline void initialize()
	{
		assert(_singleton == nullptr);
		_singleton = new VirtualMemoryManagement();
		assert(_singleton != nullptr);
	}

	static inline void shutdown()
	{
		assert(_singleton != nullptr);
		delete _singleton;
		_singleton = nullptr;
	}

	/** allocate a block of generic addresses.
	 *
	 * This region is meant to be used for allocations that can be mapped
	 * to various memory nodes (cluster or NUMA) based on a policy. So this
	 * is the pool for distributed allocations or other generic allocations.
	 */
	static inline void *allocDistrib(size_t size)
	{
		assert(_singleton != nullptr);
		return _singleton->_genericVMA->allocBlock(size);
	}

	/** allocate a block of local addresses on a NUMA node.
	 *
	 * \param size the size to allocate
	 * \param NUMAId is the the id of the NUMA node to allocate
	 */
	static inline void *allocLocalNUMA(size_t size, size_t NUMAId)
	{
		assert(_singleton != nullptr);
		VirtualMemoryArea *vma = _singleton->_localNUMAVMA.at(NUMAId);
		return vma->allocBlock(size);
	}

	//! return the NUMA node id of the node containing 'ptr' or
	//! the NUMA node count if not found
	static inline size_t findNUMA(void *ptr)
	{
		assert(_singleton != nullptr);
		for (size_t i = 0; i < _singleton->_localNUMAVMA.size(); ++i) {
			if (_singleton->_localNUMAVMA[i]->includesAddress(ptr)) {
				return i;
			}
		}

		//! Non-NUMA allocation
		return _singleton->_localNUMAVMA.size();
	}

	//! \brief Check if a region is within the distributed memory region
	//!
	//! \param[in] region the DataAccessRegion to check
	//!
	//! \return true if the region is within distributed memory
	static inline bool isDistributedRegion(DataAccessRegion const &region)
	{
		assert(_singleton != nullptr);
		return _singleton->_genericVMA->includesRange(region.getStartAddress(), region.getSize());
	}

	//! \brief Check if a memory region is (cluster) local memory
	//!
	//! \param[in] region the DataAccessRegion to check
	//!
	//! \returns true if the region is within local memory
	static inline bool isLocalRegion(DataAccessRegion const &region)
	{
		assert(_singleton != nullptr);
		for (const auto &it : _singleton->_localNUMAVMA) {
			if (it->includesRange(region.getStartAddress(), region.getSize())) {
				return true;
			}
		}

		return false;
	}

	//! \brief Check if a memory region can handled correctly by Cluster
	//!
	//! \param[in] region the DataAccessRegion to check
	//!
	//! \return true if the region is in cluster-capable memory
	static inline bool isClusterMemory(DataAccessRegion const &region)
	{
		return isDistributedRegion(region) || isLocalRegion(region);
	}

	static inline std::vector<VirtualMemoryManagement::VirtualMemoryAllocation *> getAllocations()
	{
		assert(_singleton != nullptr);
		return _singleton->_allocations;
	}

};


#endif /* __VIRTUAL_MEMORY_MANAGEMENT_HPP__ */
