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

template <typename T>
class ObjectAllocator {
public:
	using inner_type = ObjectCache<T>;

private:
	static inner_type *_cache;

public:
	static void initialize()
	{
		assert(_cache == nullptr);
		_cache = new ObjectCache<T>();
	}

	static void shutdown()
	{
		assert(_cache != nullptr);
		delete _cache;
	}

	template <typename... ARGS>
	static inline T *newObject(ARGS &&... args)
	{
		assert(_cache != nullptr);
		return _cache->newObject(std::forward<ARGS>(args)...);
	}

	static inline void deleteObject(T *ptr)
	{
		assert(_cache != nullptr);
		_cache->deleteObject(ptr);
	}
};


template <typename T> typename ObjectAllocator<T>::inner_type *ObjectAllocator<T>::_cache = nullptr;

#endif /* __OBJECT_ALLOCATOR_HPP__ */
