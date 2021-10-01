/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "MessageDataFetch.hpp"

#include <ClusterManager.hpp>
#include <MessageDelivery.hpp>

#include "executors/workflow/cluster/ExecutionWorkflowCluster.hpp"

MessageDataFetch::MessageDataFetch(
	const ClusterNode *from,
	size_t numFragments,
	std::vector<ExecutionWorkflow::ClusterDataCopyStep *> const &copySteps
)
	: Message(DATA_FETCH, sizeof(size_t) + numFragments * sizeof(DataAccessRegionInfo), from)
{
	_content = reinterpret_cast<DataFetchMessageContent *>(_deliverable->payload);

	_content->_nregions = numFragments;
	size_t index = 0;

	for (ExecutionWorkflow::ClusterDataCopyStep const *step : copySteps) {

		const std::vector<ExecutionWorkflow::FragmentInfo> &fragments = step->getFragments();

		for (ExecutionWorkflow::FragmentInfo const &fragment : fragments) {
			assert(index < numFragments);
			_content->_remoteRegionInfo[index]._remoteRegion = fragment._region;
			_content->_remoteRegionInfo[index]._id = fragment._id;

			++index;
		}
	}

	assert(index == numFragments);
}

bool MessageDataFetch::handleMessage()
{
	ClusterMemoryNode *memoryPlace = ClusterManager::getMemoryNode(getSenderId());
	assert(memoryPlace != nullptr);

	const size_t nFragments = _content->_nregions;

	for (size_t i = 0; i < nFragments; ++i) {

		DataTransfer *dt =
			ClusterManager::sendDataRaw(
				_content->_remoteRegionInfo[i]._remoteRegion,
				memoryPlace,
				_content->_remoteRegionInfo[i]._id
			);

		ClusterPollingServices::PendingQueue<DataTransfer>::addPending(dt);
	}

	return true;
}


size_t MessageDataFetch::getMessageContentSizeFromRegion(DataAccessRegion const &remoteRegion)
{
	const size_t nFragments = ClusterManager::getMPIFragments(remoteRegion);
	assert(nFragments > 0);

	return sizeof(size_t) + (nFragments * sizeof(DataAccessRegionInfo));
}


static const bool __attribute__((unused))_registered_dfetch =
	Message::RegisterMSGClass<MessageDataFetch>(DATA_FETCH);
