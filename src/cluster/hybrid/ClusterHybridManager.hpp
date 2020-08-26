/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_HYBRID_MANAGER_HPP
#define CLUSTER_HYBRID_MANAGER_HPP

class ClusterHybridManager {

private:
	static bool _inHybridClusterMode;

public:
	static void preinitialize(bool forceHybrid);

	static bool inHybridClusterMode()
	{
		return _inHybridClusterMode;
	}
};

#endif /* CLUSTER_HYBRID_MANAGER_HPP */
