/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef MPI_ERROR_HANDLER_HPP
#define MPI_ERROR_HANDLER_HPP

#pragma GCC visibility push(default)
#include <mpi.h>
#pragma GCC visibility pop

#include "lowlevel/FatalErrorHandler.hpp"

class MPIErrorHandler : public FatalErrorHandler {
private:
	static inline void printMPIError(std::ostringstream &oss, int err)
	{
		char errorString[MPI_MAX_ERROR_STRING];
		int stringLength;
		int errorClass;

		MPI_Error_class(err, &errorClass);
		MPI_Error_string(errorClass, errorString, &stringLength);
		oss << errorString << " ";

		MPI_Error_string(err, errorString, &stringLength);
		oss << errorString;
	}

public:
	template<typename... TS>
	static inline void handle(int rc, MPI_Comm comm, TS... reasonParts)
	{
		if (__builtin_expect(rc == MPI_SUCCESS, 1)) {
			return;
		}

		std::ostringstream oss;

		printMPIError(oss, rc);
		emitReasonParts(oss, reasonParts...);
		oss << std::endl;

		{
			std::lock_guard<SpinLock> guard(_errorLock);
			std::cerr << oss.str();
		}

		MPI_Abort(comm, rc);
	}

	template<typename... TS>
	static inline void handleErrorInStatus(
		int rc, MPI_Comm comm,
		int statusSize, MPI_Status *status, TS... reasonParts
	) {
		if (__builtin_expect(rc == MPI_SUCCESS, 1)) {
			return;
		}

		for (int i = 0; i < statusSize; ++i) {
			handle(status[i].MPI_ERROR, comm, reasonParts...);
		}
	}
};

#endif // MPI_ERROR_HANDLER_HPP
