/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef __VIRTUAL_MEMORY_AREA_HPP__
#define __VIRTUAL_MEMORY_AREA_HPP__

#ifndef ROUND_UP
#define ROUND_UP(_s, _r) ((((_s) + (_r) - 1) / (_r)) * (_r))
#endif /* ROUND_UP */

#include "hardware/HardwareInfo.hpp"

#include "lowlevel/FatalErrorHandler.hpp"

#include <sys/mman.h>

class VirtualMemoryArea {
	//! start address of the area
	const void *_start;

	//! size of the area
	const size_t _size;

	//! first address not belonging in the area
	const void *_end;

	//! next free pointer in the area
	char *_nextFree;

	//! amount of available memory
	size_t _available;

	size_t _count_allocs;

public:
	VirtualMemoryArea(const void *address, size_t size)
		: _start(address), _size(size), _end((char *)_start + _size),
		_nextFree((char *)_start), _available(size), _count_allocs(0)
	{
		assert(_size > 0);
		assert(_available > 0);
	}

	//! Virtual addresses should be unique, so can't copy this
	VirtualMemoryArea(VirtualMemoryArea const &) = delete;
	VirtualMemoryArea operator=(VirtualMemoryArea const &) = delete;

	~VirtualMemoryArea()
	{
	}

	inline const void *getAddress() const
	{
		return _start;
	}

	inline size_t getSize() const
	{
		return _size;
	}

	/** Returns a block of virtual address from the virtual memory area.
	 *
	 * This method is not thread-safe. Synchronization needs to be handled
	 * externaly. */
	inline void *allocBlock(size_t size)
	{
		/** Rounding up the size allocations to PAGE_SIZE is the easy
		 * way to ensure all allocations are aligned to PAGE_SIZE */
		size = ROUND_UP(size, HardwareInfo::getPageSize());
		if (size > _available) {
			FatalErrorHandler::warn(
				"VirtualMemoryArea wanted: ", size,
				" bytes but only: ", _available, " are available.",
				" vm_start: ", _start, " size: ", _size, " allocations: ", _count_allocs
			);
			return nullptr;
		}

		void *ret = (void *)_nextFree;

		_available -= size;
		_nextFree += size;
		++_count_allocs;

		return ret;
	}

	inline bool includesRange(const void *address, size_t size) const
	{
		const char *endAddress = (char *)address + size;

		return (address >= _start) && (endAddress < _end);
	}

	inline bool includesAddress(const void *address) const
	{
		return (address >= _start) && (address < _end);
	}
};

#endif /* __VIRTUAL_MEMORY_AREA_HPP__ */
