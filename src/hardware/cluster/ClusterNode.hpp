/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_NODE_HPP
#define CLUSTER_NODE_HPP

#include "hardware/places/ComputePlace.hpp"
#include "InstrumentCluster.hpp"

#include <ClusterMemoryNode.hpp>

class ClusterNode : public ComputePlace {
private:
	//! MemoryPlace associated with this cluster node
	ClusterMemoryNode *_memoryNode;

	//! This is the index of the node related to the
	//! communication layer
	int _commIndex;

	//! Name of the node for instrumentation
	std::string _instrumentationName;

	std::atomic<int> _numOffloadedTasks; // offloaded by us

public:
	ClusterNode(int index, int commIndex, int apprankNum, bool inHybridMode)
		: ComputePlace(index, nanos6_device_t::nanos6_cluster_device),
		_memoryNode(new ClusterMemoryNode(index, commIndex)),
		_commIndex(commIndex), _numOffloadedTasks(0)
	{
		assert(_memoryNode != nullptr);
		assert (_commIndex >= 0);

		//! Set the instrumentation name
		std::stringstream ss;
		if (inHybridMode) {
			ss << "g" << apprankNum << "r" << _commIndex;
		} else {
			ss << "node" << _commIndex;
		}

		_instrumentationName = ss.str();
	}

	~ClusterNode()
	{
		assert(_memoryNode != nullptr);
		delete _memoryNode;
	}

	//! \brief Get the MemoryNode of the cluster node
	inline ClusterMemoryNode *getMemoryNode() const
	{
		assert(_memoryNode != nullptr);
		return _memoryNode;
	}

	//! \brief Get the communicator index of the ClusterNode
	inline int getCommIndex() const
	{
		assert (_commIndex >= 0);
		return _commIndex;
	}

	//! \brief Get the instrumentation name
	std::string &getInstrumentationName()
	{
		return _instrumentationName;
	}

	//! \brief Update number of tasks offloaded from this node to the ClusterNode
	inline void incNumOffloadedTasks(int by)
	{
		_numOffloadedTasks += by;
		assert(_numOffloadedTasks >= 0);
	}

	//! \brief Get number of tasks offloaded from this node to the ClusterNode
	inline int getNumOffloadedTasks() const
	{
		return _numOffloadedTasks;
	}
};


#endif /* CLUSTER_NODE_HPP */
