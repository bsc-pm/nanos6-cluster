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
#include "dependencies/DataAccessType.hpp"

#include <MessageReleaseAccess.hpp>

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
		ClusterNode const *remoteNode
	);

	//! \brief Send satisfiability information to an offloaded Task
	//!
	//! \param[in] task is the offloaded task
	//! \param[in] remoteNode is the cluster node, where there is the
	//!		remote task
	//! \param[in] satInfo is the Satisfiability information we are
	//!		sending
	void sendSatisfiability(SatisfiabilityInfoMap &satInfoMap);

	//! \brief Propagate satisfiability information for a remote task
	//!
	//! \param[in] offloadedTaskId is the task identifier of the offloader
	//!		node
	//! \param[in] offloader is the clusterNode that offloaded the task
	//! \param[in] satInfo is satisfiability info we are propagating
	void propagateSatisfiabilityForHandler(
		ClusterNode const *from,
		const size_t nSatisfiabilities,
		TaskOffloading::SatisfiabilityInfo *_satisfiabilityInfo
	);

	//! \brief Release a region of an offloaded Task
	//!
	//! \param[in] task is the offloaded task
	//! \param[in] region is the DataAccessRegion to release
	//! \param[in] type is the DataAcessType of the associated DataAccess
	//! \param[in] weak is true if the associated DataAccess is weak
	//! \param[in] location is the ClusterMemoryPlace on which the access
	//!		is released
	void releaseRemoteAccess(Task *task,
		MessageReleaseAccess::ReleaseAccessInfo &releaseInfo
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
}

#endif // TASK_OFFLOADING_HPP
