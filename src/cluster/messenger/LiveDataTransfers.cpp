/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#include "LiveDataTransfers.hpp"

PaddedSpinLock<> LiveDataTransfers::_lock;
std::vector<DataTransfer *> LiveDataTransfers::_liveDataTransfers;
