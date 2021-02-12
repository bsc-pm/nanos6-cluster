/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_NULL_CLUSTER_HPP
#define INSTRUMENT_NULL_CLUSTER_HPP

#include "../api/InstrumentCluster.hpp"

#include <InstrumentInstrumentationContext.hpp>

namespace Instrument {
	inline void clusterSendMessage(Message const *, int)
	{
	}

	inline void clusterHandleMessage(Message const *, int)
	{
	}

	inline void clusterDataSend(void *, size_t, int, int, InstrumentationContext const &)
	{
	}

	inline void clusterDataReceived(void *, size_t, int, int, InstrumentationContext const &)
	{
	}

	inline void taskIsOffloaded(task_id_t, InstrumentationContext const &)
	{
	}

	inline void stateNodeNamespace(int, InstrumentationContext const &)
	{
	}

	inline void emitClusterEvent(ClusterEventType, int, InstrumentationContext const &)
	{
	}

	inline void offloadedTaskCompletes(task_id_t, InstrumentationContext const &)
	{
	}

	inline void namespacePropagation(NamespacePropagation, DataAccessRegion, InstrumentationContext const &)
	{
	}
}

#endif //! INSTRUMENT_NULL_CLUSTER_HPP
