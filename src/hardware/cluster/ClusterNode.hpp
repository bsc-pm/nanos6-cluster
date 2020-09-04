/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_NODE_HPP
#define CLUSTER_NODE_HPP

#include <atomic>
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

	//! For Extrae tracing of hybrid clusters+DLB
	int _instrumentationRank;

	//! The number of cores allocated (global) or wanted right now (local) 
	int _numAllocCores;

	//! Number of active cores available to this instance
	int _numActiveCores;

	//! The number of ready tasks reported in the utilization<n> file
	size_t _numReadyTasks;


	std::atomic<int> _numOffloadedTasks; // offloaded by us

public:
	ClusterNode(int index, int commIndex, int apprankNum, bool inHybridMode, int instrumentationRank)
		: ComputePlace(index, nanos6_device_t::nanos6_cluster_device),
		_memoryNode(new ClusterMemoryNode(index, commIndex)),
		_commIndex(commIndex), _instrumentationRank(instrumentationRank), _numAllocCores(0), _numActiveCores(0),
		_numOffloadedTasks(0) 
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

	inline void setCurrentAllocCores(int numAllocCores)
	{
		_numAllocCores = numAllocCores;
	}

	inline int getCurrentAllocCores() const
	{
		return _numAllocCores;
	}

	inline void setCurrentActiveCores(int numActiveCores)
	{
		_numActiveCores = numActiveCores;
	}

	inline int getCurrentActiveCores() const
	{
		return _numActiveCores;
	}

	inline void setCurrentReadyTasks(int numReadyTasks)
	{
		_numReadyTasks = numReadyTasks;
	}

	inline int getCurrentReadyTasks() const
	{
		return _numReadyTasks;
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

	inline int getInstrumentationRank() const
	{
		return _instrumentationRank;
	}
};


#endif /* CLUSTER_NODE_HPP */
