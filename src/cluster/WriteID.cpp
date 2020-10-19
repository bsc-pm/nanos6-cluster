/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include "WriteID.hpp"

WriteIDManager *WriteIDManager::_singleton;
CacheSet<HashID, DataAccessRegion, void *, size_t> WriteIDManager::_localWriteIDs;
