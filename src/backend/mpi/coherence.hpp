/**
 * @file
 * @brief This file implements coherence mechanisms for ArgoDSM
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#ifndef argo_coherence_hpp
#define argo_coherence_hpp argo_coherence_hpp

#include <cstddef>

void selective_si(void *addr, size_t size);

void selective_sd(void *addr, size_t size);

#endif /* argo_coherence_hpp */
