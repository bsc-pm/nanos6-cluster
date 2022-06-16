/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "MessageSatisfiability.hpp"

#include <ClusterManager.hpp>
#include <TaskOffloading.hpp>

MessageSatisfiability::MessageSatisfiability(
	TaskOffloading::SatisfiabilityInfoVector &satInfoVector
)
	: Message(SATISFIABILITY,
		sizeof(size_t) + satInfoVector.size() * sizeof(TaskOffloading::SatisfiabilityInfo))
{
	_content = reinterpret_cast<SatisfiabilityMessageContent *>(_deliverable->payload);
	_content->_nSatisfiabilities = satInfoVector.size();

	size_t index = 0;
	for (TaskOffloading::SatisfiabilityInfo const &satInfo : satInfoVector) {
		assert(index < satInfoVector.size());
		_content->_SatisfiabilityInfo[index++] = satInfo;
	}

	satInfoVector.clear();
}

bool MessageSatisfiability::handleMessage()
{
	TaskOffloading::propagateSatisfiabilityForHandler(
		ClusterManager::getClusterNode(this->getSenderId()),
		_content->_nSatisfiabilities,
		_content->_SatisfiabilityInfo
	);

	return true;
}


static const bool __attribute__((unused))_registered_satisfiability =
	Message::RegisterMSGClass<MessageSatisfiability>(SATISFIABILITY);
