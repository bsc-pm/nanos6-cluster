/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_HYBRID_INTERFACE_HPP
#define CLUSTER_HYBRID_INTERFACE_HPP

class ClusterHybridInterface {
public:
	ClusterHybridInterface()
	{
	}

	virtual ~ClusterHybridInterface()
	{
	}

	static registerHybridInterface()
	{
	}

	void updateNumbersOfCores(void)
	{
	}

	void sendUtilization(float ncores)
	{
	}
};

#endif /* CLUSTER_HYBRID_INTERFACE_HPP */
