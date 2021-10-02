/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#include "OffloadedTaskId.hpp"

std::atomic<size_t> OffloadedTaskIdManager::_counter;
