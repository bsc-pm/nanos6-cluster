/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef NANOS6_CLUSTER_H
#define NANOS6_CLUSTER_H

#include <stddef.h>

#include "major.h"

#pragma GCC visibility push(default)

// NOTE: The full version depends also on nanos6_major_api
//       That is:   nanos6_major_api . nanos6_cluster_device_api
enum nanos6_cluster_api_t { nanos6_cluster_api = 2 };

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
	//! Equally-partitioned distribution among all memory nodes
	nanos6_equpart_distribution = 0,

	//! Block distribution
	nanos6_block_distribution,

	//! Block cyclic distribution
	nanos6_cyclic_distribution
} nanos6_data_distribution_t;

typedef enum {
	//! No wait clause (cannot currently be specified with pragma clause)
	nanos6_no_wait = 0,

	//! Autowait (default for offloaded tasks if disable_autowait=false)
	nanos6_autowait,

	//! Wait (same as wait clause)
	nanos6_wait
} nanos6_early_release_t;

//! Like nanos6_library_mode_init but requires argc and argv. It is intended to be used in cluster
//! mode only
__attribute__ ((used)) char const * nanos6_library_mode_init_cluster(int argc, char **argv);


//! \brief Determine whether we are on cluster mode
//!
//! \returns true if we are on cluster mode
int nanos6_in_cluster_mode(void);

//! \brief Determine whether current node is the master node
//!
//! \returns true if the current node is the master node,
//! otherwise it returns false
int nanos6_is_master_node(void);

//! \brief Get the id of the current cluster node
//!
//! \returns the id of the current cluster node
int nanos6_get_cluster_node_id(void);

//! \brief Get the number of cluster nodes
//!
//! \returns the number of cluster nodes
int nanos6_get_num_cluster_nodes(void);

//! \brief Get if namespace propagation is enables
//!
//! \returns namespace propagation is enables.
int nanos6_get_namespace_is_enabled(void);

//! \brief Allocate distributed memory
//!
//! Distributed memory is a clsuter type of memory that can only be
//! accessed from within a task.
//!
//! \param[in] size is the size (in bytes) of distributed memory to allocate
//! \param[in] policy is the data distribution policy based on which we will
//!            distribute data across cluster nodes
//! \param[in] num_dimensions is the number of dimensions across which the data
//!            will be distributed
//! \param[in] dimensions is an array of num_dimensions elements, which contains
//!            the size of every distribution dimension
//!
//! \returns a pointer to distributed memory
void *nanos6_dmalloc(size_t size, nanos6_data_distribution_t policy, size_t num_dimensions, size_t *dimensions);

//! \brief deallocate a distributed array
//
//! \param[in] ptr is a pointer to memory previously allocated with nanos6_dmalloc
//! \param[in] size is the size of the distributed memory allocation
void nanos6_dfree(void *ptr, size_t size);

//! \brief Allocate local memory
//!
//! \param[in] size is the size (in bytes) of local memory to allocate
//!
//! \returns a pointer to local memory
void *nanos6_lmalloc(size_t size);
void *nanos6_flmalloc(size_t *size);

//! \brief Deallocate a local array
//!
//! \param[in] ptr is a pointer to memory previously allocated with nanos6_lmalloc
//! \param[in] size is the size of the local memory allocation
void nanos6_lfree(void *ptr, size_t size);

//! \brief Set early release.  Temporary until pragma clauses defined
//!
//! \param[in] early_release controls early release
void nanos6_set_early_release(nanos6_early_release_t early_release);

//! \brief Get application communicator
//!
//! \returns the communicator that can be used by the application
int nanos6_app_communicator();

#ifdef __cplusplus
}
#endif

#pragma GCC visibility pop

#endif /* NANOS6_CLUSTER_H */
