/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020-2021 Barcelona Supercomputing Center (BSC)
*/

#include "ClusterHybridManager.hpp"

bool ClusterHybridManager::_inHybridClusterMode = false;

void ClusterHybridManager::preinitialize(bool forceHybrid)
{
	_inHybridClusterMode = forceHybrid; // default policy: in hybrid if >1 apprank
}
