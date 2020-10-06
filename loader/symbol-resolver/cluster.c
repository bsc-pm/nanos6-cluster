/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#include "resolve.h"

RESOLVE_API_FUNCTION(nanos6_in_cluster_mode, "cluster", NULL);
RESOLVE_API_FUNCTION(nanos6_is_master_node, "cluster", NULL);
RESOLVE_API_FUNCTION(nanos6_get_cluster_node_id, "cluster", NULL);
RESOLVE_API_FUNCTION(nanos6_get_num_cluster_nodes, "cluster", NULL);
RESOLVE_API_FUNCTION(nanos6_get_namespace_is_enabled, "cluster", NULL);
RESOLVE_API_FUNCTION(nanos6_dmalloc, "cluster", NULL);
RESOLVE_API_FUNCTION(nanos6_dfree, "cluster", NULL);
RESOLVE_API_FUNCTION(nanos6_lmalloc, "cluster", NULL);
RESOLVE_API_FUNCTION(nanos6_flmalloc, "cluster", NULL);
RESOLVE_API_FUNCTION(nanos6_lfree, "cluster", NULL);
RESOLVE_API_FUNCTION(nanos6_set_early_release, "cluster", NULL);
RESOLVE_API_FUNCTION(nanos6_app_communicator, "cluster", NULL);
