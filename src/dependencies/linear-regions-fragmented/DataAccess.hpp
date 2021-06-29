/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef DATA_ACCESS_HPP
#define DATA_ACCESS_HPP


#define MAX_SYMBOLS 64 // TODO: Temporary solution to use a fixed bitset size

#include <atomic>
#include <bitset>
#include <cassert>
#include <set>

#include <boost/dynamic_bitset.hpp>
#include <boost/intrusive/avl_set.hpp>
#include <boost/intrusive/avl_set_hook.hpp>

#include "DataAccessLink.hpp"
#include "DataAccessObjectType.hpp"
#include "DataAccessRegion.hpp"
#include "ReductionInfo.hpp"
#include "ReductionSpecific.hpp"
#include "dependencies/DataAccessBase.hpp"
#include "executors/threads/CPUManager.hpp"
#include "lowlevel/SpinLock.hpp"

#include <InstrumentDataAccessId.hpp>
#include <InstrumentDependenciesByAccessLinks.hpp>
#include <InstrumentTaskId.hpp>
#include "cluster/WriteID.hpp"


struct DataAccess;
class Task;
class MemoryPlace;

namespace ExecutionWorkflow {
	class DataLinkStep;
};

#define VALID_NAMESPACE_UNKNOWN -1
#define VALID_NAMESPACE_NONE -2

//! The accesses that one or more tasks perform sequentially to a memory location that can occur concurrently (unless commutative).
struct DataAccess : protected DataAccessBase {
	friend struct TaskDataAccessLinkingArtifacts;

private:
	enum status_bit_coding {
		REGISTERED_BIT = 0,

		COMPLETE_BIT,

		READ_SATISFIED_BIT,
		WRITE_SATISFIED_BIT,
		CONCURRENT_SATISFIED_BIT,
		COMMUTATIVE_SATISFIED_BIT,
		RECEIVED_REDUCTION_INFO_BIT,
		ALLOCATED_REDUCTION_INFO_BIT,
		RECEIVED_SLOT_SET_BIT,
		CLOSES_REDUCTION_BIT,

		READ_SATISFIABILITY_PROPAGATION_INHIBITED_BIT,
		CONCURRENT_SATISFIABILITY_PROPAGATION_INHIBITED_BIT,
		COMMUTATIVE_SATISFIABILITY_PROPAGATION_INHIBITED_BIT,
		REDUCTION_INFO_PROPAGATION_INHIBITED_BIT,

		HAS_SUBACCESSES_BIT,
		IN_BOTTOM_MAP_BIT,
#ifndef NDEBUG
		IS_REACHABLE_BIT,
		HAS_BEEN_DISCOUNTED_BIT,
#endif
		PROPAGATED_IN_REMOTE_NAMESPACE,
		PROPAGATE_FROM_NAMESPACE_BIT,
		PROPAGATED_NAMESPACE_INFO_BIT,
		EARLY_RELEASE_IN_NAMESPACE_BIT,
		REMOTE_HAS_PSEUDOWRITE_BIT,
		DISABLE_READ_PROPAGATION_UNTIL_HERE,
		TOTAL_STATUS_BITS
	};

public:
	typedef std::bitset<TOTAL_STATUS_BITS> status_t;
	typedef std::bitset<MAX_SYMBOLS> symbols_t;

private:
	DataAccessObjectType _objectType;

	//! The region of data covered by the access
	DataAccessRegion _region;

	status_t _status;

#ifdef USE_CLUSTER
	WriteID _writeID;
#endif

	//! Direct next access
	DataAccessLink _next;

	//! An index that determines the data type and the operation of the reduction (if applicable)
	reduction_type_and_operator_index_t _reductionTypeAndOperatorIndex;

	//! A bitmap of the "symbols" this access is related to
	symbols_t _symbols;

	//! An index that identifies the reduction within the task (if applicable)
	reduction_index_t _reductionIndex;

	//! Reduction-specific information of current access
	ReductionInfo *_reductionInfo;

	//! Reduction-specific information of previous access
	ReductionInfo *_previousReductionInfo;

	//! Reduction slots that may contain uncombined data belonging to the
	//! reduction this access is part of (if applicable)
	boost::dynamic_bitset<> _reductionSlotSet;

	//! Location of the DataAccess
	MemoryPlace const *_location;

	//! Output memory location of the access
	MemoryPlace const *_outputLocation;

	//! DataLinkStep related with this data access
	ExecutionWorkflow::DataLinkStep *_dataLinkStep;

	int _validNamespacePrevious;
	int _validNamespaceSelf;
	Task *_namespacePredecessor;
	Task *_namespaceSuccessor;

public:
	DataAccess(
		DataAccessObjectType objectType,
		DataAccessType type, bool weak,
		Task *originator,
		DataAccessRegion accessRegion,
		reduction_type_and_operator_index_t reductionTypeAndOperatorIndex,
		reduction_index_t reductionIndex,
		MemoryPlace const *location = nullptr,
		MemoryPlace const *outputLocation = nullptr,
		ExecutionWorkflow::DataLinkStep *dataLinkStep = nullptr,
		Instrument::data_access_id_t instrumentationId = Instrument::data_access_id_t(),
		status_t status = 0,
		DataAccessLink next = DataAccessLink()
	)
		: DataAccessBase(type, weak, originator, instrumentationId),
		_objectType(objectType),
		_region(accessRegion),
		_status(status),
#ifdef USE_CLUSTER
		_writeID(0),
#endif
		_next(next),
		_reductionTypeAndOperatorIndex(reductionTypeAndOperatorIndex),
		_reductionIndex(reductionIndex),
		_reductionInfo(nullptr),
		_previousReductionInfo(nullptr),
		_location(location),
		_outputLocation(outputLocation),
		_dataLinkStep(dataLinkStep),
		_validNamespacePrevious(VALID_NAMESPACE_UNKNOWN),
		_validNamespaceSelf(VALID_NAMESPACE_UNKNOWN),
		_namespacePredecessor(nullptr),
		_namespaceSuccessor(nullptr)
	{
		assert(originator != nullptr);

		if (_type == REDUCTION_ACCESS_TYPE) {
			_reductionSlotSet.resize(ReductionInfo::getMaxSlots());
		}
	}

	DataAccess(const DataAccess &other)
		: DataAccessBase(
			other.getType(),
			other.isWeak(),
			other.getOriginator(),
			Instrument::data_access_id_t()
		),
		_objectType(other.getObjectType()),
		_region(other.getAccessRegion()),
		_status(other.getStatus()),
#ifdef USE_CLUSTER
		_writeID(other._writeID),
#endif // USE_CLUSTER
		_next(other.getNext()),
		_reductionTypeAndOperatorIndex(other.getReductionTypeAndOperatorIndex()),
		_reductionIndex(other.getReductionIndex()),
		_reductionInfo(other.getReductionInfo()),
		_previousReductionInfo(other.getPreviousReductionInfo()),
		_reductionSlotSet(other.getReductionSlotSet()),
		_location(other.getLocation()),
		_outputLocation(other.getOutputLocation()),
		_dataLinkStep(other.getDataLinkStep()),
		_validNamespacePrevious(other.getValidNamespacePrevious()),
		_validNamespaceSelf(other.getValidNamespaceSelf()),
		_namespacePredecessor(other.getNamespacePredecessor()),
		_namespaceSuccessor(other.getNamespaceSuccessor())
	{}

	~DataAccess()
	{
		Instrument::removedDataAccess(_instrumentationId);
		assert(hasBeenDiscounted());
	}

	inline DataAccessObjectType getObjectType() const
	{
		return _objectType;
	}

	inline DataAccessType getType() const
	{
		return _type;
	}

	inline bool isWeak() const
	{
		return _weak;
	}

	inline Task *getOriginator() const
	{
		return _originator;
	}

	inline void setNewInstrumentationId(Instrument::task_id_t const &taskInstrumentationId)
	{
		_instrumentationId = Instrument::createdDataAccess(
			nullptr,
			_type, _weak, _region,
			/* Read Satisfied */ false, /* Write Satisfied */ false, /* Globally Satisfied */ false,
			(Instrument::access_object_type_t) _objectType,
			taskInstrumentationId
		);
	}

	inline void setUpNewFragment(Instrument::data_access_id_t originalAccessInstrumentationId)
	{
		_instrumentationId = Instrument::fragmentedDataAccess(originalAccessInstrumentationId, _region);
	}

	inline bool upgrade(bool newWeak, DataAccessType newType)
	{
		if ((newWeak != _weak) || (newType != _type)) {
			bool oldWeak = _weak;
			DataAccessType oldType = _type;

			bool wasSatisfied = satisfied();

			_weak = newWeak;
			_type = newType;

			Instrument::upgradedDataAccess(
				_instrumentationId,
				oldType, oldWeak,
				newType, newWeak,
				wasSatisfied && !satisfied()
			);

			return true;
		}

		return false;
	}

	status_t const &getStatus() const
	{
		return _status;
	}

#ifdef USE_CLUSTER
	WriteID getWriteID() const
	{
		return _writeID;
	}

	void setWriteID(WriteID id)
	{
		_writeID = id;
	}
#else
	WriteID getWriteID() const
	{
		return 0;
	}

#endif // USE_CLUSTER

	void setRegistered()
	{
		assert(!isRegistered());
		_status[REGISTERED_BIT] = true;
	}
	bool isRegistered() const
	{
		return _status[REGISTERED_BIT];
	}
	void clearRegistered()
	{
		// No assertion here since it is a clear method instead of an unset method
		_status[REGISTERED_BIT] = false;
	}

	void setComplete()
	{
		assert(!complete());
		_status[COMPLETE_BIT] = true;
		Instrument::completedDataAccess(_instrumentationId);
	}
	bool complete() const
	{
		return _status[COMPLETE_BIT];
	}

	void setReadSatisfied(MemoryPlace const *location = nullptr)
	{
		assert(!readSatisfied());
		_status[READ_SATISFIED_BIT] = true;
		setLocation(location);
		Instrument::newDataAccessProperty(_instrumentationId, "RSat", "Read Satisfied");
	}
	bool readSatisfied() const
	{
		return _status[READ_SATISFIED_BIT];
	}

	void setWriteSatisfied()
	{
		assert(!writeSatisfied());
		_status[WRITE_SATISFIED_BIT] = true;
		Instrument::newDataAccessProperty(_instrumentationId, "WSat", "Write Satisfied");
	}
	bool writeSatisfied() const
	{
		return _status[WRITE_SATISFIED_BIT];
	}

	void setConcurrentSatisfied()
	{
		assert(!concurrentSatisfied());
		_status[CONCURRENT_SATISFIED_BIT] = true;
		Instrument::newDataAccessProperty(_instrumentationId, "ConSat", "Concurrent Satisfied");
	}
	bool concurrentSatisfied() const
	{
		return _status[CONCURRENT_SATISFIED_BIT];
	}

	void setCommutativeSatisfied()
	{
		assert(!commutativeSatisfied());
		_status[COMMUTATIVE_SATISFIED_BIT] = true;
		Instrument::newDataAccessProperty(_instrumentationId, "CommSat", "Commutative Satisfied");
	}
	bool commutativeSatisfied() const
	{
		return _status[COMMUTATIVE_SATISFIED_BIT];
	}

	void setPropagatedInRemoteNamespace()
	{
		assert(!propagatedInRemoteNamespace());
		_status[PROPAGATED_IN_REMOTE_NAMESPACE] = true;
	}

	bool propagatedInRemoteNamespace() const
	{
		return _status[PROPAGATED_IN_REMOTE_NAMESPACE];
	}

	void setReceivedReductionInfo()
	{
		assert(!receivedReductionInfo());
		_status[RECEIVED_REDUCTION_INFO_BIT] = true;
		Instrument::newDataAccessProperty(_instrumentationId, "RIRec", "ReductionInfo Received");
	}
	bool receivedReductionInfo() const
	{
		return _status[RECEIVED_REDUCTION_INFO_BIT];
	}

	void setAllocatedReductionInfo()
	{
		assert(_type == REDUCTION_ACCESS_TYPE);
		assert(!allocatedReductionInfo());
		_status[ALLOCATED_REDUCTION_INFO_BIT] = true;
		Instrument::newDataAccessProperty(_instrumentationId, "RIAlloc", "ReductionInfo Allocated");
	}
	bool allocatedReductionInfo() const
	{
		return _status[ALLOCATED_REDUCTION_INFO_BIT];
	}

	void setReceivedReductionSlotSet()
	{
		assert(!receivedReductionSlotSet());
		_status[RECEIVED_SLOT_SET_BIT] = true;
		Instrument::newDataAccessProperty(_instrumentationId, "RSetRec", "Reduction Set Received");
	}
	bool receivedReductionSlotSet() const
	{
		return _status[RECEIVED_SLOT_SET_BIT];
	}

	void setClosesReduction()
	{
		assert(_type == REDUCTION_ACCESS_TYPE);
		assert(!closesReduction());
		_status[CLOSES_REDUCTION_BIT] = true;
		Instrument::newDataAccessProperty(_instrumentationId, "Rc", "Closes Reduction");
	}
	bool closesReduction() const
	{
		return _status[CLOSES_REDUCTION_BIT];
	}

	bool canPropagateReadSatisfiability() const
	{
		return !_status[READ_SATISFIABILITY_PROPAGATION_INHIBITED_BIT];
	}
	void unsetCanPropagateReadSatisfiability()
	{
		assert(canPropagateReadSatisfiability());
		_status[READ_SATISFIABILITY_PROPAGATION_INHIBITED_BIT] = true;
	}

	bool canPropagateConcurrentSatisfiability() const
	{
		return !_status[CONCURRENT_SATISFIABILITY_PROPAGATION_INHIBITED_BIT];
	}
	void unsetCanPropagateConcurrentSatisfiability()
	{
		assert(canPropagateConcurrentSatisfiability());
		_status[CONCURRENT_SATISFIABILITY_PROPAGATION_INHIBITED_BIT] = true;
	}

	bool canPropagateCommutativeSatisfiability() const
	{
		return !_status[COMMUTATIVE_SATISFIABILITY_PROPAGATION_INHIBITED_BIT];
	}
	void unsetCanPropagateCommutativeSatisfiability()
	{
		assert(canPropagateCommutativeSatisfiability());
		_status[COMMUTATIVE_SATISFIABILITY_PROPAGATION_INHIBITED_BIT] = true;
	}

	bool canPropagateReductionInfo() const
	{
		return !_status[REDUCTION_INFO_PROPAGATION_INHIBITED_BIT];
	}
	void unsetCanPropagateReductionInfo()
	{
		assert(canPropagateReductionInfo());
		_status[REDUCTION_INFO_PROPAGATION_INHIBITED_BIT] = true;
	}

	void setHasSubaccesses()
	{
		assert(!hasSubaccesses());
		_status[HAS_SUBACCESSES_BIT] = true;
	}
	void unsetHasSubaccesses()
	{
		assert(hasSubaccesses());
		_status[HAS_SUBACCESSES_BIT] = false;
	}
	bool hasSubaccesses() const
	{
		return _status[HAS_SUBACCESSES_BIT];
	}

	void setInBottomMap()
	{
		assert(!isInBottomMap());
		_status[IN_BOTTOM_MAP_BIT] = true;
	}
	void unsetInBottomMap()
	{
		assert(isInBottomMap());
		_status[IN_BOTTOM_MAP_BIT] = false;
	}
	bool isInBottomMap() const
	{
		return _status[IN_BOTTOM_MAP_BIT];
	}

	void setLocation(MemoryPlace const *location)
	{
		_location = location;
		Instrument::newDataAccessLocation(_instrumentationId, location);
	}
	MemoryPlace const *getLocation() const
	{
		return _location;
	}
	bool hasLocation() const
	{
		return (_location != nullptr);
	}

	void setOutputLocation(MemoryPlace const *location)
	{
		_outputLocation = location;
	}
	MemoryPlace const *getOutputLocation() const
	{
		return _outputLocation;
	}
	bool hasOutputLocation() const
	{
		return (_outputLocation != nullptr);
	}

	//! Set the DataLinkStep of the access. The access must not
	//! have a DataLinkStep already
	void setDataLinkStep(ExecutionWorkflow::DataLinkStep *step)
	{
		assert(!hasDataLinkStep());
		assert(step != nullptr);

		_dataLinkStep = step;
	}

	//! Unset the DataLinkStep of the access. The access must
	//! have a DataLinkStep already
	void unsetDataLinkStep()
	{
		assert(hasDataLinkStep());

		_dataLinkStep = nullptr;
	}

	//! Get the DataLinkStep of the access.
	ExecutionWorkflow::DataLinkStep *getDataLinkStep() const
	{
		return _dataLinkStep;
	}

	//! Check if the access has a DataLinkStep
	bool hasDataLinkStep() const
	{
		return (_dataLinkStep != nullptr);
	}

#ifndef NDEBUG
	void setReachable()
	{
		assert(!isReachable());
		_status[IS_REACHABLE_BIT] = true;
	}
	bool isReachable() const
	{
		return _status[IS_REACHABLE_BIT];
	}
#endif

	void markAsDiscounted()
	{
#ifndef NDEBUG
		assert(!_status[HAS_BEEN_DISCOUNTED_BIT]);
		_status[HAS_BEEN_DISCOUNTED_BIT] = true;
#endif
		Instrument::dataAccessBecomesRemovable(_instrumentationId);
	}

#ifndef NDEBUG
	bool hasBeenDiscounted() const
	{
		return _status[HAS_BEEN_DISCOUNTED_BIT];
	}
#endif

	void inheritFragmentStatus(DataAccess const *other)
	{
		if (other->readSatisfied()) {
			setReadSatisfied(other->getLocation());
		}
		if (other->writeSatisfied()) {
			setWriteSatisfied();
		}
		if (other->concurrentSatisfied()) {
			setConcurrentSatisfied();
		}
		if (other->commutativeSatisfied()) {
			setCommutativeSatisfied();
		}
		if (other->receivedReductionInfo()) {
			setReceivedReductionInfo();
			setPreviousReductionInfo(other->getPreviousReductionInfo());
		}
		if (other->getReductionInfo() != nullptr) {
			setReductionInfo(other->getReductionInfo());
		}
		if (other->allocatedReductionInfo()) {
			assert(other->getReductionInfo() != nullptr);
			setAllocatedReductionInfo();
		}
		if (other->receivedReductionSlotSet()) {
			setReceivedReductionSlotSet();
		}
		if (other->getReductionSlotSet().size() > 0) {
			setReductionSlotSet(other->getReductionSlotSet());
		}
		if (other->complete()) {
			setComplete();
		}
		setValidNamespaceSelf(other->getValidNamespaceSelf());
		setValidNamespacePrevious(VALID_NAMESPACE_NONE, nullptr);
	}

	DataAccessRegion const &getAccessRegion() const
	{
		return _region;
	}

	void setAccessRegion(DataAccessRegion const &newRegion)
	{
		_region = newRegion;
		if (_instrumentationId != Instrument::data_access_id_t()) {
			Instrument::modifiedDataAccessRegion(_instrumentationId, _region);
		}
	}


	bool satisfied() const
	{
		if (_type == READ_ACCESS_TYPE) {
			return readSatisfied();
		} else if (_type == CONCURRENT_ACCESS_TYPE) {
			return concurrentSatisfied();
		} else if (_type == COMMUTATIVE_ACCESS_TYPE) {
			return commutativeSatisfied();
		} else if (_type == REDUCTION_ACCESS_TYPE) {
			// Semantic note: For a reduction access to be satisfied it means
			// that the necessary structures for executing and combining the
			// reduction have been received and that the original region can be
			// safely accessed for combination
			return writeSatisfied()
				&& (allocatedReductionInfo() || (receivedReductionInfo() && receivedReductionSlotSet()));
		} else {
			return readSatisfied() && writeSatisfied();
		}
	}


	bool hasNext() const
	{
		return (_next._task != nullptr);
	}
	void setNext(DataAccessLink const &next)
	{
		_next = next;
	}
	DataAccessLink const &getNext() const
	{
		return _next;
	}

	reduction_type_and_operator_index_t getReductionTypeAndOperatorIndex() const
	{
		return _reductionTypeAndOperatorIndex;
	}

	reduction_index_t getReductionIndex() const
	{
		return _reductionIndex;
	}

	ReductionInfo *getReductionInfo() const
	{
		return _reductionInfo;
	}

	void setReductionInfo(ReductionInfo *reductionInfo)
	{
		assert(_reductionInfo == nullptr);
		assert(_type == REDUCTION_ACCESS_TYPE);
		_reductionInfo = reductionInfo;
	}

	ReductionInfo *getPreviousReductionInfo() const
	{
		return _previousReductionInfo;
	}

	void setPreviousReductionInfo(ReductionInfo *previousReductionInfo)
	{
		assert(_previousReductionInfo == nullptr);
		_previousReductionInfo = previousReductionInfo;
	}

	boost::dynamic_bitset<> const &getReductionSlotSet() const
	{
		return _reductionSlotSet;
	}

	boost::dynamic_bitset<> &getReductionSlotSet()
	{
		return _reductionSlotSet;
	}

	void setReductionSlotSet(const boost::dynamic_bitset<> &reductionSlotSet)
	{
		assert(_reductionSlotSet.none());
		_reductionSlotSet = reductionSlotSet;
	}

	void setReductionAccessedSlot(size_t slotIndex)
	{
		_reductionSlotSet.set(slotIndex);
	}

	Instrument::data_access_id_t const &getInstrumentationId() const
	{
		return _instrumentationId;
	}

	Instrument::data_access_id_t &getInstrumentationId()
	{
		return _instrumentationId;
	}

	bool isInSymbol(int symbol) const
	{
		return _symbols[symbol];
	}
	void addToSymbol(int symbol)
	{
		_symbols.set(symbol);
	}
	void removeFromSymbol(int symbol)
	{
		_symbols.reset(symbol);
	}
	void addToSymbols(const symbols_t &symbols)
	{
		_symbols |= symbols;
	}
	symbols_t getSymbols() const
	{
		return _symbols;
	}

	// Get the namespace node id for the previous access.
	// This is needed in order to send the correct hint to the remote node.
	int getValidNamespacePrevious() const
	{
		return _validNamespacePrevious;
	}

	// Get the namespace node id for this access itself. It is the node
	// on which the task is executed. Consider putting this information
	// in the Task rather than the DataAccess.
	int getValidNamespaceSelf() const
	{
		return _validNamespaceSelf;
	}

	Task *getNamespacePredecessor() const
	{
		return _namespacePredecessor;
	}

	// Set the namespace nodeId and predecessor task for the previous access.
	// validNamespace: nodeId where task was last offloaded
	// namespacePredecessor: last task offloaded to that node
	void setValidNamespacePrevious(int validNamespacePrevious, Task *namespacePredecessor)
	{
		_validNamespacePrevious = validNamespacePrevious;
		_namespacePredecessor = namespacePredecessor;
	}

	// Set the namespace node id for this access itself. It is the node
	// on which the task is executed, and it will be passed through the
	// dependency system to the successor. Consider putting this information
	// in the Task rather than the DataAccess.
	void setValidNamespaceSelf(int validNamespaceSelf)
	{
		_validNamespaceSelf = validNamespaceSelf;
	}

	Task *getNamespaceSuccessor() const
	{
		return _namespaceSuccessor;
	}

	void setNamespaceSuccessor(Task *namespaceSuccessor)
	{
		_namespaceSuccessor = namespaceSuccessor;
	}

	bool getPropagatedNamespaceInfo() const
	{
		return _status[PROPAGATED_NAMESPACE_INFO_BIT];
	}

	void setPropagatedNamespaceInfo()
	{
		_status[PROPAGATED_NAMESPACE_INFO_BIT] = true;
	}

	bool getPropagateFromNamespace() const
	{
		return _status[PROPAGATE_FROM_NAMESPACE_BIT];
	}

	void setPropagateFromNamespace()
	{
		_status[PROPAGATE_FROM_NAMESPACE_BIT] = true;
	}

	bool getEarlyReleaseInNamespace() const
	{
		return _status[EARLY_RELEASE_IN_NAMESPACE_BIT];
	}

	void setEarlyReleaseInNamespace()
	{
		_status[EARLY_RELEASE_IN_NAMESPACE_BIT] = true;
	}

	bool remoteHasPseudowrite() const
	{
		return _status[REMOTE_HAS_PSEUDOWRITE_BIT];
	}

	void setRemoteHasPseudowrite()
	{
		_status[REMOTE_HAS_PSEUDOWRITE_BIT] = true;
	}

	bool disableReadPropagationUntilHere() const
	{
		return _status[DISABLE_READ_PROPAGATION_UNTIL_HERE];
	}

	void setDisableReadPropagationUntilHere()
	{
		_status[DISABLE_READ_PROPAGATION_UNTIL_HERE] = true;
	}

	inline bool canMergeWith(const DataAccess *other) const 
	{
		// Most of the below checks are clearly necessary in order to be
		// able to merge the accesses. Regarding the previous access'
		// namespace (validNamespacePrevious), this information is not
		// really needed after the task starts, so it is not needed at any
		// time that the unfragment functions are called. But each access
		// always receives the namespace previous exactly once and we
		// cannot delete the access until this information has been
		// received. We know that it has been received once
		// getValidNamespacePrevious returns something other than
		// VALID_NAMESPACE_UNKNOWN.  The below check (that in fact both
		// accesses have the same value of the namespace previous) is
		// slightly more than is required, but it is simple.
		return (other != nullptr)
			&& this->getAccessRegion().getStartAddress() == other->getAccessRegion().getEndAddress()
			&& this->getStatus() == other->getStatus()
			&& this->isWeak() == other->isWeak()
			&& this->getType() == other->getType()
			&& other->getLocation()
			&& other->getLocation()->isClusterLocalMemoryPlace()
			&& this->getLocation()
			&& this->getLocation()->isClusterLocalMemoryPlace()
			&& this->getLocation()->isDirectoryMemoryPlace() == other->getLocation()->isDirectoryMemoryPlace()
			&& this->getDataLinkStep() == other->getDataLinkStep()
			&& this->getNext()._task == other->getNext()._task
			&& this->getNext()._objectType == other->getNext()._objectType
			&& this->getValidNamespacePrevious() == other->getValidNamespacePrevious()
			&& this->getNamespaceSuccessor() == other->getNamespaceSuccessor();
	}

};


#endif // DATA_ACCESS_HPP
