/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_STATS_HPP
#define CLUSTER_STATS_HPP

class ClusterStats {
public:
	static inline void initialize()
	{
	}

	static inline void markCPUIdle()
	{
	}

	static inline void markCPUBusy()
	{
	}

	static inline busy_cores_t getCoresBusy() 
	{
	}

	static inline busy_cores_t clearTimeBusy()
	{
	}
};



#endif /* CLUSTER_STATS_HPP */
