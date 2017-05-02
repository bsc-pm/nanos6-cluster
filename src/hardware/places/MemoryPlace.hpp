#ifndef MEMORY_PLACE_HPP
#define MEMORY_PLACE_HPP

#include <vector>
#include <map>
#include "memory/AddressSpace.hpp"

class ComputePlace;

class MemoryPlace {
protected:
	AddressSpace * _addressSpace;
	int _index;	
	
public:
	MemoryPlace(int index, AddressSpace * addressSpace = nullptr)
		: _addressSpace(addressSpace), _index(index)
	{}
	
	virtual ~MemoryPlace() {}
	inline int getIndex(void){ return _index; } 
	inline AddressSpace * getAddressSpace(){ return _addressSpace; } 
};

#endif //MEMORY_PLACE_HPP