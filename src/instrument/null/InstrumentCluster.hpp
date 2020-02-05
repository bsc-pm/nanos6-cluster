/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_NULL_CLUSTER_HPP
#define INSTRUMENT_NULL_CLUSTER_HPP

#include "../api/InstrumentCluster.hpp"

#include <InstrumentInstrumentationContext.hpp>

namespace Instrument {
	inline void clusterMessageInitSend(Message const *, int,
			InstrumentationContext const &)
	{
	}

	inline void clusterMessageCompleteSend(Message const *,
			InstrumentationContext const &)
	{
	}

	inline void enterHandleReceivedMessage(Message const *, int,
			InstrumentationContext const &)
	{
	}

	inline void exitHandleReceivedMessage(Message const *,
			InstrumentationContext const &)
	{
	}
}

#endif //! INSTRUMENT_NULL_CLUSTER_HPP
