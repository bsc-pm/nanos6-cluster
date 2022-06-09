/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef NOEAGERSEND_INFO_HPP
#define NOEAGERSEND_INFO_HPP

#include "OffloadedTaskId.hpp"

namespace TaskOffloading {

	struct AccessInfo {
		//! The region
		DataAccessRegion _region;

		//! The offloaded task ID that doesn't access the data
		OffloadedTaskIdManager::OffloadedTaskId _offloadedTaskId;

		//! Disable eager send for this region
		bool _noEagerSend;

		//! An allmemory access is in fact only read
		bool _isReadOnly;

		AccessInfo(
			DataAccessRegion region,
			OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId,
			bool noEagerSend,
			bool isReadOnly
		) : _region(region), _offloadedTaskId(offloadedTaskId), _noEagerSend(noEagerSend), _isReadOnly(isReadOnly)
		{}

		friend std::ostream& operator<<(std::ostream& out, const AccessInfo& accessInfo)
		{
			out << accessInfo._region
				<< " task-id: " << accessInfo._offloadedTaskId
				<< " unused: " << accessInfo._noEagerSend
				<< " read-only: " << accessInfo._isReadOnly;
			return out;
		}
	};

	typedef std::vector<AccessInfo> AccessInfoVector;
	typedef std::map<ClusterNode *, std::vector<AccessInfo>> AccessInfoMap;
}


#endif /* NOEAGERSEND_INFO_HPP */
