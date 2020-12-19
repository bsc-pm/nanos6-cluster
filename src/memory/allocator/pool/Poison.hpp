#ifndef ASAN_HPP
#define ASAN_HPP

/*
 * Support for AddressSanitizer in the memory pool allocators.
 *
 * Configure using ./configure LDFLAGS="-ldl -lasan -fsanitize=address -fno-omit-frame-pointer"
 * CFLAGS="-fsanitize=address -fno-omit-frame-pointer -ggdb" CPPFLAGS="-fsanitize=address
 * -fno-omit-frame-pointer -ggdb"
 */

#ifdef __SANITIZE_ADDRESS__
extern "C" {
	void __asan_poison_memory_region(void const volatile *addr, size_t size);
	void __asan_unpoison_memory_region(void const volatile *addr, size_t size);
}
#endif

namespace AddressSanitizer {

#ifdef __SANITIZE_ADDRESS__

	inline void poisonMemoryRegion(void const volatile *addr, size_t size)
	{
		__asan_poison_memory_region(addr, size);
	}

	inline void unpoisonMemoryRegion(void const volatile *addr, size_t size)
	{
		__asan_unpoison_memory_region(addr, size);
	}

#else

	inline void poisonMemoryRegion(void const volatile *, size_t)
	{
	}

	inline void unpoisonMemoryRegion(void const volatile *, size_t)
	{
	}

#endif

}

#endif /* ASAN_HPP */
