/**
 * @file
 * @brief This file implements coherence mechanisms for ArgoDSM
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#ifndef argo_coherence_hpp
#define argo_coherence_hpp argo_coherence_hpp

#include <cstddef>
/**
 * @brief Selectively self-invalidates the memory region given
 */
void selective_si(void *addr, size_t size);

/**
 * @brief Selectively self-downgrades the memory region given
 */
void selective_sd(void *addr, size_t size);

/**
 * @brief Selectively self-downgrades the memory region given
 * @pre The entire memory region must be data-race-free
 */
void selective_sd_region(void *addr, size_t size);

#endif /* argo_coherence_hpp */
