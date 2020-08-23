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

inline void poison_memory_region(void const volatile *addr, size_t size)
{
#ifdef __SANITIZE_ADDRESS__
	__asan_poison_memory_region(addr, size);
#else
	(void)addr;
	(void)size;
#endif
}

inline void unpoison_memory_region(void const volatile *addr, size_t size)
{
#ifdef __SANITIZE_ADDRESS__
	__asan_unpoison_memory_region(addr, size);
#else
	(void)addr;
	(void)size;
#endif
}

#endif /* ASAN_HPP */
