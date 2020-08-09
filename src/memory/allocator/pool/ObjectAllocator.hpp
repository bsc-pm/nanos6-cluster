/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2017 Barcelona Supercomputing Center (BSC)
*/

#ifndef __OBJECT_ALLOCATOR_HPP__
#define __OBJECT_ALLOCATOR_HPP__

#include <DataAccess.hpp>
#include <ReductionInfo.hpp>
#include <BottomMapEntry.hpp>
#include <ObjectCache.hpp>

#include <InstrumentMemory.hpp>

template <typename T>
class ObjectAllocator {
public:
	using inner_type = ObjectCache<T>;

private:
	static inner_type *_cache;
	static Instrument::instrument_allocator_t<T> _instrumentAllocator;

public:
	static void initialize()
	{
		_instrumentAllocator.allocatorInitialize();
		_cache = new ObjectCache<T>();
	}

	static void shutdown()
	{
		delete _cache;
		_instrumentAllocator.allocatorShutdown();
	}

	template <typename... ARGS>
	static inline T *newObject(ARGS &&... args)
	{
		_instrumentAllocator.allocateObject();
		return _cache->newObject(std::forward<ARGS>(args)...);
	}

	static inline void deleteObject(T *ptr)
	{
		_instrumentAllocator.deallocateObject();
		_cache->deleteObject(ptr);
	}
};


template <typename T> typename ObjectAllocator<T>::inner_type *ObjectAllocator<T>::_cache = nullptr;
template <typename T> Instrument::instrument_allocator_t<T> ObjectAllocator<T>::_instrumentAllocator;


#endif /* __OBJECT_ALLOCATOR_HPP__ */
