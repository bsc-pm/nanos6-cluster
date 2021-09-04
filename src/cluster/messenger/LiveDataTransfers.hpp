/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef LIVE_DATA_TRANSFERS_HPP
#define LIVE_DATA_TRANSFERS_HPP

#include <functional>
#include <mutex>
#include <vector>
#include "lowlevel/PaddedSpinLock.hpp"
#include <DataAccessRegion.hpp>

class DataTransfer;

class LiveDataTransfers {

	static PaddedSpinLock<> _lock;
	static std::vector<DataTransfer *> _liveDataTransfers;

public:

	static void add(DataTransfer *dataTransfer)
	{
		std::lock_guard<PaddedSpinLock<>> guard(_lock);
		_liveDataTransfers.push_back(dataTransfer);
	}

	static void remove(DataTransfer *dataTransfer)
	{
		std::lock_guard<PaddedSpinLock<>> guard(_lock);
		auto it = std::find(_liveDataTransfers.begin(), _liveDataTransfers.end(), dataTransfer);
		assert(it != _liveDataTransfers.end());
		_liveDataTransfers.erase(it);
	}

	static bool check(
		std::function<bool(DataTransfer *)> checkPending,
		std::function<DataTransfer *()> createNew)
	{
		std::lock_guard<PaddedSpinLock<>> guard(_lock);
		for(DataTransfer *dataTransfer : _liveDataTransfers) {
			bool done = checkPending(dataTransfer);
			if (done) {
				/* Return done flag */
				return true;
			}
		}
		/* Return not done */
		DataTransfer *dataTransferNew = createNew();
		_liveDataTransfers.push_back(dataTransferNew);
		return false;
	}
};

#endif /* LIVE_DATA_TRANSFERS_HPP */
