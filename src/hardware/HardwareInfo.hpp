#ifndef HARDWARE_INFO_HPP
#define HARDWARE_INFO_HPP
#include <vector>
#include <map>
#include <atomic>

#include "places/MemoryPlace.hpp"
#include "places/ComputePlace.hpp"

class HardwareInfo {
private:
	typedef std::map<int, MemoryPlace*> MemoryNodes_t;
	typedef std::map<int, ComputePlace*> ComputeNodes_t;
    //! pair.first -> Index of ComputePlace || pair.second -> Index of MemoryPlace
    //typedef std::map<std::pair<int, int>, float> Distances_t;
	
    // \brief Physical memory places. For instance, 2 NUMA nodes are 2 different memory places.
    static MemoryNodes_t _memoryNodes;
    // \brief Logical compute places.
    static ComputeNodes_t _computeNodes;
    // \brief Cost of accessing from a ComputePlace to a MemoryPlace.
    //Distances_t _distances;
	//! \brief Number of initialized CPUs
    //! TODO: atomic required?
	static /*std::atomic<long>*/ long _totalCPUs;
    //! \brief Preloaded pagesize to avoid system calls.
	static long _pageSize;

public:
    // Generic methods
    static void initialize();
	static inline long getPageSize(void) { return _pageSize; }

    // ComputeNodes related methods
	static inline size_t getComputeNodeCount(void) { return _computeNodes.size(); }
	static inline ComputePlace* getComputeNode(int index) { return _computeNodes[index]; }
    static std::vector<int> getComputeNodeIndexes();
	static std::vector<ComputePlace*> getComputeNodes();

    // MemoryNodes related methods
	static inline size_t getMemoryNodeCount(void) { return _memoryNodes.size(); }
	static inline MemoryPlace* getMemoryNode(int index) { return _memoryNodes[index]; }
    static std::vector<int> getMemoryNodeIndexes();
	static std::vector<MemoryPlace*> getMemoryNodes();
};

#endif // HARDWARE_INFO_HPP
