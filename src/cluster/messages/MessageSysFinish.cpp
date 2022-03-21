/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#include "MessageSysFinish.hpp"
#include "cluster/ClusterManager.hpp"

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

	ClusterManager::finishClusterNamespace();

	return true;
}
