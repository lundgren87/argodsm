/**
 * @file
 * @brief This file provides the full C interface to ArgoDSM
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#ifndef argo_argo_h
#define argo_argo_h argo_argo_h

#include <stddef.h>

#include "allocators/allocators.h"
#include "synchronization/synchronization.h"

/**
 * @brief initialize ArgoDSM system
 * @param argo_size The desired size of the global memory in bytes, or 0. If the
 *                  value is specified as 0, then the value in environment
 *                  variable @ref ARGO_MEMORY_SIZE is used instead.
 *                  If @ref ARGO_MEMORY_SIZE is unset, then a default is used.
 * @param cache_size The desired size of the local cache in bytes, or 0. If the
 *                   value is specified as 0, then the value in environment
 *                   variable @ref ARGO_CACHE_SIZE is used instead.
 *                   If @ref ARGO_CACHE_SIZE is unset, then a default is used.
 */
void argo_init(size_t argo_size, size_t cache_size);

/**
 * @brief shut down ArgoDSM system
 */
void argo_finalize();

/**
 * @brief reset ArgoDSM system
 * @warning this is mainly intended for testing, not for public use
 */
void argo_reset();

/**
 * @brief A unique ArgoDSM node identifier. Counting starts from 0.
 * @return The node id
 */
int  argo_node_id();

/**
 * @brief Number of ArgoDSM nodes being run
 * @return The total number of ArgoDSM nodes
 */
int  argo_number_of_nodes();

#endif /* argo_argo_h */
