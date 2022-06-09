/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef DATA_TRANSFER_HPP
#define DATA_TRANSFER_HPP

#include <functional>

#include "hardware/places/MemoryPlace.hpp"

#include <DataAccessRegion.hpp>

#include <InstrumentCluster.hpp>

#include "LiveDataTransfers.hpp"

#include "TransferBase.hpp"

class MemoryPlace;

class DataTransfer : public TransferBase {
public:
	//! The region that is being transfered
	const DataAccessRegion _region;

	//! Source memory place
	const MemoryPlace *_source;

	//! Target memory place
	const MemoryPlace *_target;

	//! ID as the message index for instrumenting the transferred region
	const int _id;

	//! rank of MPI source for instrumenting the transferred region (non-blocking case)
	const int _MPISource;

	//! Is it a data fetch
	const bool _isFetch;

public:
	DataTransfer(
		const DataAccessRegion &region,
		const MemoryPlace *source,
		const MemoryPlace *target,
		void *messengerData,
		int MPISource,
		int id,
		bool isFetch
	) : TransferBase(messengerData),
		_region(region), _source(source), _target(target), _id(id), _MPISource(MPISource),
		_isFetch(isFetch)
	{}

	virtual ~DataTransfer()
	{}

	inline size_t getSize() const
	{
		return _region.getSize();
	}

	inline DataAccessRegion const getDataAccessRegion() const
	{
		return _region;
	}

	inline MemoryPlace const *getSource() const
	{
		return _source;
	}

	inline MemoryPlace const *getTarget() const
	{
		return _target;
	}

	//! \brief Return the message index for instrumenting a transferred region
	inline int getMessageId() const
	{
		return _id;
	}

	//! \brief Mark the DataTransfer as completed
	//!
	//! If there is a valid callback assigned to the DataTransfer it will
	//! be invoked
	inline void markAsCompleted() override
	{
		if (_isFetch) {
			Instrument::clusterDataReceived(_region.getStartAddress(), _region.getSize(), _MPISource, _id);
			// Important: remove from live data transfers before calling the callbacks (otherwise
			// callbacks could potentially be lost)
			LiveDataTransfers::remove(this);
		}

		TransferBase::markAsCompleted();

		if (_isFetch) {
			Instrument::clusterDataReceived(_region.getStartAddress(), _region.getSize(), _MPISource, -1);
		}
	}


	friend std::ostream& operator<<(std::ostream &out, const DataTransfer &dt)
	{
		out << "DataTransfer from: " <<
			dt._source->getIndex() << " to: " <<
			dt._target->getIndex() << " region:" <<
			dt._region;
		return out;
	}
};


#endif /* DATA_TRANSFER_HPP */
