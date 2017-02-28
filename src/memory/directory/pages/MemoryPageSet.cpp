#include "MemoryPageSet.hpp"

#include "hardware/HardwareInfo.hpp"

#include <numaif.h>
#include <iostream>
#include <cerrno>

MemoryPageSet::MemoryPageSet(): BaseType(){}


MemoryPageSet::iterator MemoryPageSet::find(void *address){
    return BaseType::find( DataAccessRange( address, address ) );
}

void MemoryPageSet::insert(DataAccessRange range){
	// Guarantee that the range is not on the AVL already

	long pagesize = HardwareInfo::getPageSize();
	//void *page = (void *)( (long) range.getStartAddress() & ~(pagesize-1) );
	char *page = (char *)((long) range.getStartAddress() & ~(pagesize-1));
	size_t size = static_cast<char *>(range.getEndAddress()) - static_cast<char *>(page);
	
	int npages = 1 + ((size-1) / pagesize); // Ceil the division

	void * pages[npages];
	int status[npages]; 
	
	pages[0] = (void *)page;
	for(int i = 1; i < npages; i++){
		page += pagesize;
		pages[i] = (void *)page;
	}


	int err = move_pages(0, npages, pages, nullptr, status, 0);
    if(err != 0) {
        std::perror("move_pages failed");
        assert(err==0);
    }
    if(status[0] < 0) {
        return;
        //errno = status[0];
        //std::perror("move_pages failed");
        //assert(status[0] >= 0);
    }

	// Find the previous page if it is registered
	MemoryPageSet::iterator edge = lower_bound(static_cast<char *>( pages[0] ) - pagesize);
	MemoryPageObject *obj = nullptr;

	
	if(edge != BaseType::begin()){
		edge--;
	} 
	
	// Check if the new pages are adjacent to the previous ones and they share the same status for a merge
	if(edge != BaseType::end()){
		if(edge->getEndAddress() == pages[0] && edge->getLocation() == status[0]){
			obj = &(*edge);
			BaseType::erase(*edge); //Needed for now to avoid duplicates
			obj->setEndAddress( static_cast<void *>( static_cast<char *>( obj->getEndAddress() ) + pagesize ) ); // Extend the object
		}	
	} 
	
	// Create a new page
	if(obj == nullptr){
		obj = new MemoryPageObject(pages[0], pagesize, status[0]);
	}	


	for(int i = 1; i < npages; i++){
		// If the next page is on a different node, insert object and create a new one for the node
		if((obj->getLocation() != status[i]) && (obj->getLocation() >= 0)){
			BaseType::insert(*obj); // No need to delete if repeated, since it can only be an object retrieved from the dictionary
			
			obj = new MemoryPageObject(pages[i], pagesize, status[i]);
		// If the location is the same, extend the current object
		} else {
			obj->setEndAddress( static_cast<void *>( static_cast<char *>( obj->getEndAddress() ) + pagesize ) );
		}
	}

	
	edge = BaseType::find( DataAccessRange( obj->getEndAddress(), obj->getEndAddress() ) );
	if(edge != BaseType::end() && edge->getLocation() == obj->getLocation()){
		edge->setStartAddress( obj->getStartAddress() );
		if(obj != &(*edge) ){
			delete obj;
		}
	} else {
        if(obj->getLocation() >= 0)
            BaseType::insert(*obj);
	}
	
}
