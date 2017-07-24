#ifndef RESOLVE_H
#define RESOLVE_H


#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif


#include "loader.h"

#include "api/nanos6.h"

#include <assert.h>
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>


static void *_nanos6_resolve_symbol(char const *fname, char const *area, char const *fallback)
{
	if (__builtin_expect(_nanos6_lib_handle == NULL, 0)) {
		_nanos6_loader();
		if (__builtin_expect(_nanos6_lib_handle == NULL, 0)) {
			fprintf(stderr, "Nanos 6 loader error: call to %s before library initialization\n", fname);
			abort();
		}
	}
	
	void *symbol = dlsym(_nanos6_lib_handle, fname);
	dlerror();
	if ((symbol == NULL) && (fallback != NULL)) {
		symbol = dlsym(_nanos6_lib_handle, fallback);
		dlerror();
		if (symbol != NULL) {
			fprintf(stderr, "Nanos 6 loader warning: %s runtime function %s is undefined in '%s' falling back to function %s instead\n", area, fname, _nanos6_lib_filename, fallback);
		}
	}
	if (symbol == NULL) {
		fprintf(stderr, "Nanos 6 loader error: %s runtime function %s is undefined in '%s'\n", area, fname, _nanos6_lib_filename);
		abort();
	}
	
	return symbol;
}


static void *_nanos6_resolve_symbol_with_local_fallback(char const *fname, char const *area, void *fallback, char const *fallback_name)
{
	if (__builtin_expect(_nanos6_lib_handle == NULL, 0)) {
		_nanos6_loader();
		if (__builtin_expect(_nanos6_lib_handle == NULL, 0)) {
			fprintf(stderr, "Nanos 6 loader error: call to %s before library initialization\n", fname);
			abort();
		}
	}
	
	void *symbol = dlsym(_nanos6_lib_handle, fname);
	dlerror();
	if (symbol == NULL) {
		symbol = fallback;
		if (symbol != NULL) {
			fprintf(stderr, "Nanos 6 loader warning: %s runtime function %s is undefined in '%s' falling back to function %s instead\n", area, fname, _nanos6_lib_filename, fallback_name);
		}
	}
	
	return symbol;
}


static void *_nanos6_resolve_intercepted_symbol_with_global_fallback(char const *fname, char const *iname, char const *area)
{
	if (__builtin_expect(_nanos6_lib_handle == NULL, 0)) {
		static void *symbol = NULL;
		
		if (symbol == NULL) {
			symbol = dlsym(RTLD_NEXT, fname);
			dlerror();
		}
		
		return symbol;
	}
	
	static void *symbol = NULL;
	if (symbol == NULL) {
		symbol = dlsym(_nanos6_lib_handle, iname);
		dlerror();
	}
	if (symbol == NULL) {
		symbol = dlsym(RTLD_NEXT, fname);
		dlerror();
		if (symbol == NULL) {
			fprintf(stderr, "Nanos 6 loader error: %s intercepted function %s is undefined in '%s'\n", area, fname, _nanos6_lib_filename);
			abort();
		}
	}
	
	return symbol;
}


#define DECLARE_LIBC_FALLBACK(prefix, fname, rtype, ...) \
extern rtype prefix##fname (__VA_ARGS__)

#define DECLARE_INTERCEPTED_FUNCTION_POINTER(sname, fname, rtype, ...) \
	rtype (*sname)(__VA_ARGS__) = (rtype (*)(__VA_ARGS__)) NULL;

#define RESOLVE_INTERCEPTED_FUNCTION_WITH_LIBC_FALLBACK(prefix, sname, fname, rtype, ...) \
	if (__builtin_expect(_nanos6_lib_handle == NULL, 0)) { \
		sname = (rtype (*)(__VA_ARGS__)) prefix##fname; \
	} else { \
		\
		static volatile __thread int resolvingSymbol = 0; \
		static rtype (*nanos6_symbol)(__VA_ARGS__) = NULL; \
		if ((nanos6_symbol == NULL) && !resolvingSymbol) { \
			resolvingSymbol++; \
			nanos6_symbol = (rtype (*)(__VA_ARGS__)) dlsym(_nanos6_lib_handle, "nanos6_intercepted_" #fname); \
			dlerror(); \
			resolvingSymbol--; \
		} \
		if (nanos6_symbol == NULL) { \
			nanos6_symbol = (rtype (*)(__VA_ARGS__)) prefix##fname; \
		} \
		\
		sname = nanos6_symbol; \
	} \
	1


#endif /* RESOLVE_H */
