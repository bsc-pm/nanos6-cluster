/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef MPI_DATA_TRANSFER_HPP
#define MPI_DATA_TRANSFER_HPP

#pragma GCC visibility push(default)
#include <mpi.h>
#pragma GCC visibility pop

#include "../DataTransfer.hpp"

class MPIDataTransfer : public DataTransfer {

public:
	MPIDataTransfer(
		DataAccessRegion const &region,
		MemoryPlace const *source,
		MemoryPlace const *target,
		MPI_Request *request,
		int MPISource,
		int transferId,
		bool isFetch
	) : DataTransfer(region, source, target, request, MPISource, transferId, isFetch)
	{
	}

	~MPIDataTransfer()
	{
	}
};

#endif /* MPI_DATA_TRANSFER_HPP */
