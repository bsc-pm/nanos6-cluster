/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_HYBRID_INTERFACE_HPP
#define CLUSTER_HYBRID_INTERFACE_HPP

#include <time.h>
#include <support/GenericFactory.hpp>
#include "lowlevel/FatalErrorHandler.hpp"

class ClusterHybridInterface {

	protected:
	public:
		ClusterHybridInterface()
		{
		}

		virtual ~ClusterHybridInterface()
		{
		}

		virtual void initialize(int externalRank,
								int apprankNum,
								int internalRank,
								int physicalNodeNum,
								int indexThisPhysicalNode,
								int clusterSize,
								const std::vector<int> &internalRankToExternalRank,
								const std::vector<int> &instanceThisNodeToExternalRank) = 0;

		//! Write out the information about this rank: apprankNum, internalRank, etc.
		virtual void writeMapFile() = 0;

		//! Called by polling service
		virtual void poll() = 0;
};

#define REGISTER_HYBIF_CLASS(NAME, CREATEFN) \
	GenericFactory<std::string, ClusterHybridInterface*>::getInstance().emplace(NAME, CREATEFN)

#endif /* CLUSTER_HYBRID_INTERFACE_HPP */
