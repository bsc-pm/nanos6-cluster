/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019--2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef DATA_SEND_INFO_HPP
#define DATA_SEND_INFO_HPP

#include <map>
#include <vector>

class MemoryPlace;

namespace TaskOffloading {

	struct DataSendRegionInfo {

		//! The remote region we update
		DataAccessRegion _remoteRegion;

		//! Node that should receive the data
		const MemoryPlace *_recipient;

		//! Id of the resulting data transfer message
		int _id;
	};

	typedef std::vector<DataSendRegionInfo> DataSendRegionInfoVector;
	typedef std::map<ClusterNode *, std::vector<DataSendRegionInfo>> DataSendRegionInfoMap;
}


#endif /* DATA_SEND_INFO_HPP */
