/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020-2021 Barcelona Supercomputing Center (BSC)
*/

#include <functional>
#include "ClusterHybridManager.hpp"

bool ClusterHybridManager::_inHybridClusterMode = false;
ClusterHybridInterface *ClusterHybridManager::_hyb = nullptr;

void ClusterHybridManager::preinitialize(bool forceHybrid, int externalRank, int apprankNum)
{
	_inHybridClusterMode = forceHybrid; // default policy: in hybrid if >1 apprank

	_hyb = GenericFactory<std::string, ClusterHybridInterface*>::getInstance().create("hybrid-file-interface");
	assert(_hyb != nullptr);

	/*
	 * Initialize hybrid interface: controls data distribution within the apprank
	 */
	_hyb->initialize(externalRank, apprankNum);
}
