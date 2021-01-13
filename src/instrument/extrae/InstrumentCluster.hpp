/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_EXTRAE_CLUSTER_HPP
#define INSTRUMENT_EXTRAE_CLUSTER_HPP

#if HAVE_CONFIG_H
#include <config.h>
#endif

#include "../api/InstrumentCluster.hpp"


namespace Instrument {

#ifdef USE_CLUSTER
	void defineClusterExtraeEvents();
#else
	inline void defineClusterExtraeEvents()
	{
	}
#endif

}

#endif //! INSTRUMENT_EXTRAE_CLUSTER_HPP
