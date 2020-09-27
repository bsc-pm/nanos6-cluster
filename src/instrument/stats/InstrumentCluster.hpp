/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_STATS_CLUSTER_HPP
#define INSTRUMENT_STATS_CLUSTER_HPP

#include "../api/InstrumentCluster.hpp"

#include <InstrumentInstrumentationContext.hpp>

namespace Instrument {

	void initClusterCounters();

	inline void clusterMessageCompleteSend(Message const *, InstrumentationContext const &)
	{
	}

	inline void exitHandleReceivedMessage(Message const *, InstrumentationContext const &)
	{
	}

	inline void taskIsOffloaded(task_id_t, InstrumentationContext const &)
	{
	}

	inline void emitClusterEvent(ClusterEventType, int, InstrumentationContext const &)
	{
	}

	inline void offloadedTaskCompletes(task_id_t, InstrumentationContext const &)
	{
	}

	void showClusterCounters(std::ofstream &output);

	inline void stateNodeNamespace(int, InstrumentationContext const &)
	{
	}
}

#endif //! INSTRUMENT_STATS_CLUSTER_HPP
