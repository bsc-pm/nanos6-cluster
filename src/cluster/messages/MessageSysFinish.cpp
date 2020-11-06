/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#include "MessageSysFinish.hpp"
#include "cluster/ClusterShutdownCallback.hpp"
#include "cluster/ClusterManager.hpp"
#include "cluster/NodeNamespace.hpp"

#include <nanos6/bootstrap.h>

MessageSysFinish::MessageSysFinish(const ClusterNode *from)
	: Message(SYS_FINISH, 1, from)
{}

bool MessageSysFinish::handleMessage()
{
	FatalErrorHandler::failIf(
		ClusterManager::isMasterNode(),
		"Master node received a MessageSysFinish; this should never happen."
	);

	ClusterShutdownCallback *callback;
	do {
		//! We will spin to avoid the (not very likely) case that the Callback has not been set
		//! yet. This could happen if we received and handled a MessageSysFinish before the loader
		//! code has finished setting up everything.
		//! Same situation can happen if the master node sends this message too early and the remote
		//! namespace has not started yet.
		callback = ClusterManager::getShutdownCallback();
	} while (!callback && !NodeNamespace::isEnabled());

	if (NodeNamespace::isEnabled()) {
		NodeNamespace::shutdown();
	} else {
		callback->execute();
	}

	//! Synchronize with all other cluster nodes at this point
	//! Master node makes this in ClusterManager::shutdownPhase1
	ClusterManager::synchronizeAll();

	return true;
}
