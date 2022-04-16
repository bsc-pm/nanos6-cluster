/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef NOEAGERSEND_INFO_HPP
#define NOEAGERSEND_INFO_HPP

#include "OffloadedTaskId.hpp"

namespace TaskOffloading {

	struct NoEagerSendInfo {
		//! The region we do not access
		DataAccessRegion _region;

		//! The offloaded task ID that doesn't access the data
		OffloadedTaskIdManager::OffloadedTaskId _offloadedTaskId;

		NoEagerSendInfo(
			DataAccessRegion region,
			OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId
		) : _region(region), _offloadedTaskId(offloadedTaskId)
		{}
	};

	typedef std::vector<NoEagerSendInfo> NoEagerSendInfoVector;
	typedef std::map<ClusterNode *, std::vector<NoEagerSendInfo>> NoEagerSendInfoMap;
}


#endif /* NOEAGERSEND_INFO_HPP */
