/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/


#ifndef CLUSTERUTIL_H
#define CLUSTERUTIL_H

#include <execinfo.h> // for backtrace
#include <dlfcn.h>    // for dladdr
#include <cxxabi.h>   // for __cxa_demangle

#include <cstdio>
#include <cstdlib>
#include <string>
#include <sstream>
#include <limits>

//#include <cassert>

#include <ClusterManager.hpp>

#define bufsize 1024
#define stacksize 128

// this is a glibc function used by assert internally.
extern void __assert_fail(
	const char *__assertion,
	const char *__file,
	unsigned int __line,
	const char *__function
) __THROW __attribute__ ((__noreturn__));

inline void __cluster_assert_fail(
	const char *__assertion,
	const char *__file,
	unsigned int __line,
	const char *__function
) {
	char tmp[bufsize];
	snprintf(tmp, bufsize, "Node(%d) %s",
		ClusterManager::getCurrentClusterNode()->getIndex(),
		__file
	);
	__assert_fail (__assertion, tmp, __line, __function);
}

// In the verbose instrumentation there is the logMessage, but functional only when verbose
#define clusteFprintf(STREAM, FORMAT, ...)				     	 \
	fprintf(STREAM, "# e%dg%di%d " FORMAT,				     	 \
			ClusterManager::getExternalRank(),                   \
			ClusterManager::getApprankNum(),                     \
			ClusterManager::getCurrentClusterNode()->getIndex(), \
			##__VA_ARGS__)

// Print Node [Rest]
#define clusterPrintf(FORMAT, ...) clusteFprintf(stdout, FORMAT, ##__VA_ARGS__)

// Print Node file:line [Rest]
#define clusterVPrintf(FORMAT, ...)										\
	clusteFprintf(stdout, "%s:%d " FORMAT, __FILE__, __LINE__,__VA_ARGS__)

#define clusterAssert(expr)												\
	(static_cast <bool> (expr)											\
		? void (0)														\
		: __cluster_assert_fail (#expr, __FILE__, __LINE__, __ASSERT_FUNCTION))


#define clusterCout std::cout << "# e" << ClusterManager::getExternalRank() \
							  << "g" << ClusterManager::getApprankNum() \
							  << "i" << ClusterManager::getCurrentClusterNode()->getIndex() << " "

// This function produces a stack backtrace with demangled function & method names.
inline std::string clusterBacktrace()
{
	void *callstack[stacksize];
	int nFrames = backtrace(callstack, stacksize);
	char **symbols = backtrace_symbols(callstack, nFrames);

	std::ostringstream trace_buf;
	char buffer[bufsize];

	for (int i = 1; i < nFrames; i++) { // Start in one to skip this function
		Dl_info info;

		if (dladdr(callstack[i], &info) != 0
			&& info.dli_sname != NULL
			&& info.dli_saddr != NULL) {

			char *demangled = NULL;
			int status = -1;

			if (info.dli_sname[0] == '_') {
				demangled = abi::__cxa_demangle(info.dli_sname, NULL, 0, &status);
			}

			snprintf(buffer, bufsize, "\t%-3d %*p %s + %zd\n",
				i,
				int(2 + sizeof(void*) * 2),
				callstack[i],
				status == 0 ? demangled : info.dli_sname == 0 ? symbols[i] : info.dli_sname,
				(char *)callstack[i] - (char *)info.dli_saddr
			);

			free(demangled);
		} else {
			snprintf(buffer, bufsize, "\t%-3d %*p %s\n",
				i,
				int(2 + sizeof(void*) * 2),
				callstack[i],
				symbols[i]
			);
		}
		trace_buf << buffer;
	}

	free(symbols);

	if (nFrames >= stacksize) {
		trace_buf << "[truncated]\n";
	}
	return trace_buf.str();
}


#endif /* CLUSTERUTIL_H */
