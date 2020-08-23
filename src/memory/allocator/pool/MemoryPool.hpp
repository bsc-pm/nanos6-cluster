/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2017 Barcelona Supercomputing Center (BSC)
*/

#ifndef MEMORY_POOL_HPP
#define MEMORY_POOL_HPP

#include "MemoryPoolGlobal.hpp"

#include "Poison.hpp"

#define NEXT_CHUNK(_r) *((void **)_r)

class MemoryPool {
private:
	// There is one pool per CPU. No need to lock
	MemoryPoolGlobal * const _globalAllocator;
	const size_t _chunkSize;
	void *_topChunk;

	MemoryPool() = delete;

public:
	MemoryPool(MemoryPoolGlobal *globalAllocator, size_t chunkSize)
		: _globalAllocator(globalAllocator),
		_chunkSize(chunkSize),
		_topChunk(nullptr)
	{
		assert (_chunkSize > 0);
		assert (_globalAllocator != nullptr);
	}

	void *getChunk()
	{
		if (_topChunk == nullptr) { // Fill Pool
			size_t globalChunkSize;
			_topChunk = _globalAllocator->getMemory(_chunkSize, globalChunkSize);

			// If globalChunkSize % _chunkSize != 0, some memory will be left unused
			const size_t numChunks = globalChunkSize / _chunkSize;

			FatalErrorHandler::failIf(
				numChunks == 0,
				"Memory returned from global pool is smaller than chunk size (", _chunkSize, "B)"
			);

			void *prevChunk = _topChunk;
			for (size_t i = 1; i < numChunks; ++i) {
				// Link chunks to each other, by writing a pointer to
				// the next chunk in this chunk
				NEXT_CHUNK(prevChunk) = (char *)_topChunk + (i * _chunkSize);
				prevChunk = (char *)_topChunk + (i * _chunkSize);
			}

			NEXT_CHUNK(prevChunk) = nullptr;

			// Poison whole region
			poison_memory_region(_topChunk, globalChunkSize);
		}

		void *chunk = _topChunk;

		// Unpoison chunk
		unpoison_memory_region(chunk, _chunkSize);
		_topChunk = NEXT_CHUNK(chunk);

		return chunk;
	}

	void returnChunk(void *chunk)
	{
		NEXT_CHUNK(chunk) = _topChunk;
		_topChunk = chunk;

		// Poison now it is in the linked list
		poison_memory_region(chunk, _chunkSize);
	}
};

#endif // MEMORY_POOL_HPP
