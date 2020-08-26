/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef TASK_OFFLOADING_HPP
#define TASK_OFFLOADING_HPP

#include <cstddef>
#include <sstream>
#include <vector>

#include "SatisfiabilityInfo.hpp"
#include "DataSendInfo.hpp"
#include "dependencies/DataAccessType.hpp"

#include <MessageReleaseAccess.hpp>
#include <NoEagerSendInfo.hpp>

class ClusterNode;
class MemoryPlace;
class MessageTaskNew;
class Task;

namespace TaskOffloading {

	//! \brief Offload a Task to a remote ClusterNode
	//!
	//! \param[in] task is the Task we are offloading
	//! \param[in] satInfo is the sastisfiability info that we already know
	//		about the task already
	//! \param[in] remoteNode is the ClusterNode to which we are offloading
	void offloadTask(
		Task *task,
		SatisfiabilityInfoVector const &satInfo,
		ClusterNode *remoteNode
	);

	//! \brief Send satisfiability information to an offloaded Task
	//!
	//! \param[in] task is the offloaded task
	//! \param[in] remoteNode is the cluster node, where there is the
	//!		remote task
	//! \param[in] satInfo is the Satisfiability information we are
	//!		sending
	void sendSatisfiabilityAndDataSends(SatisfiabilityInfoMap &satInfoMap, DataSendRegionInfoMap &regionInfoMap);

	//! \brief Propagate satisfiability information for a remote task
	void propagateSatisfiabilityForHandler(
		ClusterNode const *from,
		const size_t nSatisfiabilities,
		TaskOffloading::SatisfiabilityInfo *_satisfiabilityInfo
	);

	//! \brief Release a region of an offloaded Task
	void releaseRemoteAccessForHandler(
		Task *task,
		const size_t nRegions,
		MessageReleaseAccess::ReleaseAccessInfo *regionInfoList
	);

	//! Create and submit a task
	void remoteTaskCreateAndSubmit(MessageTaskNew *msg, Task *parent, bool useCallbackInContext);

	//! \brief Create and submit a remote task
	//!
	//! \param[in] args the MessageTaskNew of the remote Task as a void pointer
	void remoteTaskWrapper(void *args);

	//! \brief Completion callback for a remote task
	//!
	//! \param[in] args the MessageTaskNew of the remote Task as a void pointer
	void remoteTaskCleanup(void *args);

	//! \brief Send no eager send message
	void sendNoEagerSend(Task *task, const std::vector<NoEagerSendInfo> &regions);

	//! \brief Handle disable eager send of data for offloaded task's dependency that is not accessed
	void receivedNoEagerSend(Task *task, DataAccessRegion region);
}

#endif // TASK_OFFLOADING_HPP
