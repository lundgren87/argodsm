/**
 * @file
 * @brief This file implements coherence mechanisms for ArgoDSM
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#include "coherence.hpp"
#include "swdsm.h"

/* EXTERNAL VARIABLES FROM BACKEND */
/**@todo might declare the variables here later when we remove the old backend */
extern control_data *cacheControl;
extern unsigned long *globalSharers;
extern pthread_mutex_t cachemutex;
extern sem_t ibsem;
extern void * startAddr;
extern char * barwindowsused;
extern MPI_Win *globalDataWindow;
extern argo_statistics stats;

static const unsigned int pagesize = 4096;

/**
 * @brief Selectively self-invalidate the memory region given by addr and size
 * @param addr The starting address of the memory region to invalidate
 * @param size The size of the memory region to invalidate
 */
void selective_si(void *addr, size_t size){
    unsigned long i, p, glob_addr, id, homenode, homenode_offset, classidx, idx;
    double t1, t2;
    double cachet1, cachet2;

    if(size == 0){
        return;
    }

    char *address = (char*)addr;
    unsigned long lineindex_start =  (unsigned long)((unsigned long)(address) - (unsigned long)(startAddr));
    unsigned long lineindex_end = lineindex_start + size - 1;
    lineindex_start/=(pagesize*CACHELINE);
    lineindex_end/=(pagesize*CACHELINE);

    t1 = MPI_Wtime();
    /*
     *Get the offset from the start of the global address space, lets call this 'global address'
     */
    cachet1 = MPI_Wtime();
    pthread_mutex_lock(&cachemutex);
    cachet2 = MPI_Wtime();
    stats.cachemutextime += cachet2-cachet1;
    stats.cachemutextime_ssi += cachet2-cachet1;
    sem_wait(&ibsem);
    for(p = lineindex_start; p <= lineindex_end; p++, address+=pagesize){
        glob_addr =  (unsigned long)((unsigned long)(address) - (unsigned long)(startAddr));	
        glob_addr/=(pagesize*CACHELINE);
        glob_addr*=(pagesize*CACHELINE);

        id = 1 << getID();	
        homenode = getHomenode(glob_addr);
        homenode_offset = getOffset(glob_addr);
        classidx = get_classification_index(glob_addr);
        idx = getCacheIndex(glob_addr);

        argo_byte dirty = cacheControl[idx].dirty;
        if(dirty == DIRTY){
            /**@todo should only write back this page*/
            cacheControl[idx].dirty = CLEAN;
            for(i = 0; i <CACHELINE; i++){
                storepageDIFF(idx+i,glob_addr+pagesize*i);
            }

        }

        /* TODO: This part causes stalling on MPI_Win_Unlock()
         * in loadcacheline or prefetchcacheline functions */
//        if(
//        // node is single writer
//        (globalSharers[classidx+1]==id)
//        ||
//        // No writer and assert that the node is a sharer
//        ((globalSharers[classidx+1]==0) && ((globalSharers[classidx]&id)==id))
//        ){
//            //nothing - we keep the pages, SD is done in flushWB
//        }
//        else{ //multiple writer or SO
            cacheControl[idx].dirty=CLEAN;
            cacheControl[idx].state = INVALID;
            mprotect((char*)startAddr + glob_addr, pagesize*CACHELINE, PROT_NONE);
//        }	
    }

    // Make sure to sync writebacks
    for(i = 0; i < argo_get_nodes(); i++){
        if(barwindowsused[i] == 1){
            MPI_Win_unlock(i, globalDataWindow[i]); //Sync write backs
            barwindowsused[i] = 0;
        }
    }
    t2 = MPI_Wtime();
    stats.ssitime += t2-t1;
    sem_post(&ibsem);
    pthread_mutex_unlock(&cachemutex);
}


/**
 * @brief Selectively self-downgrade the memory region given by addr and size
 * @param addr The starting address of the memory region to downgrade
 * @param size The size of the memory region to downgrade
 */
void selective_sd(void *addr, size_t size){
    unsigned long i, p ,glob_addr, idx;
    double t1, t2;
    double cachet1, cachet2;

    if(size == 0){
        return;
    }

    char *address = (char*)addr;
    // Get the start and end indexes of the memory region
    unsigned long lineindex_start =  (unsigned long)((unsigned long)(address) - (unsigned long)(startAddr));
    unsigned long lineindex_end = lineindex_start + size - 1;
    lineindex_start/=(pagesize*CACHELINE);
    lineindex_end/=(pagesize*CACHELINE);

    t1 = MPI_Wtime();
    // Ensure exclusive cache access
    cachet1 = MPI_Wtime();
    pthread_mutex_lock(&cachemutex);
    cachet2 = MPI_Wtime();
    stats.cachemutextime += cachet2-cachet1;
    stats.cachemutextime_ssd += cachet2-cachet1;
    sem_wait(&ibsem);

    // Iterate over each page to self-downgrade
    for(p = lineindex_start; p <= lineindex_end; p++, address+=pagesize){
        // Get the offset from the start of the global address space
        glob_addr =  (unsigned long)((unsigned long)(address) - (unsigned long)(startAddr));	
        glob_addr/=(pagesize*CACHELINE);
        glob_addr*=(pagesize*CACHELINE);

        idx = getCacheIndex(glob_addr);

        // Write back this page if it is DIRTY and set it to CLEAN
        argo_byte dirty = cacheControl[idx].dirty;
        if(dirty == DIRTY){
            mprotect((char*)startAddr + glob_addr, pagesize*CACHELINE, PROT_READ);
            cacheControl[idx].dirty = CLEAN;
            for(i = 0; i <CACHELINE; i++){
                storepageDIFF(idx+i,glob_addr+pagesize*i);
            }
        }
    }

    // Make sure to sync writebacks
    for(i = 0; i < argo_get_nodes(); i++){
        if(barwindowsused[i] == 1){
            MPI_Win_unlock(i, globalDataWindow[i]); //Sync write backs
            barwindowsused[i] = 0;
        }
    }
    t2 = MPI_Wtime();
    stats.ssdtime += t2-t1;
    // Release exclusive cache access
    sem_post(&ibsem);
    pthread_mutex_unlock(&cachemutex);
}


/**
 * @brief Selectively self-downgrade the memory region given by addr and size.
 * @pre The region given by addr and size must be contiguous
 * @pre There may be no other writer than the invoking node on the entire region
 * @param addr The starting address of the memory region to downgrade
 * @param size The size of the memory region to downgrade
 */
void selective_sd_region(void *addr, size_t size){
    unsigned long i, p, homenode, offset, glob_addr, idx, regionstart, regionend;
    double t1, t2;
    double cachet1, cachet2;

    if(size == 0){
        return;
    }

    char *address = (char*)addr;
    // Get the start and end indexes of the memory region
    regionstart = (unsigned long)addr;
    regionend = (unsigned long)((unsigned long)address+(unsigned long)size);
    unsigned long lineindex_start =  (unsigned long)((unsigned long)(address) - (unsigned long)(startAddr));
    unsigned long lineindex_end = lineindex_start + size - 1;
    lineindex_start/=(pagesize*CACHELINE);
    lineindex_end/=(pagesize*CACHELINE);

    t1 = MPI_Wtime();
    // Ensure exclusive cache access
    cachet1 = MPI_Wtime();
    pthread_mutex_lock(&cachemutex);
    cachet2 = MPI_Wtime();
    stats.cachemutextime += cachet2-cachet1;
    stats.cachemutextime_ssd += cachet2-cachet1;
    sem_wait(&ibsem);

    // Iterate over each page to self-downgrade
    for(p = lineindex_start; p <= lineindex_end; p++, address+=pagesize){
        // Get the offset from the start of the global address space
        glob_addr=(unsigned long)((unsigned long)(address) - (unsigned long)(startAddr));	
        glob_addr/=(pagesize*CACHELINE);
        glob_addr*=(pagesize*CACHELINE);

        offset = getOffset(glob_addr);
        homenode = getHomenode(glob_addr);
        idx = getCacheIndex(glob_addr);

        // Write back this page if it is DIRTY and set it to CLEAN
        argo_byte dirty = cacheControl[idx].dirty;
        if(dirty == DIRTY){
            mprotect((char*)startAddr + glob_addr, pagesize*CACHELINE, PROT_READ);
            cacheControl[idx].dirty = CLEAN;

            // If it is the first or last page and start or end are not page aligned
            // Alternatively, if the size is smaller than pagesize
            if( (size<pagesize) ||
                (p == lineindex_start && regionstart%pagesize != 0) || 
                (p == lineindex_end && regionend%pagesize != 0)) {
                // Create and send a diff for the page
                for(i = 0; i <CACHELINE; i++){
                    storepageDIFF(idx+i,glob_addr+pagesize*i);
                }
            }
            // If the whole page is in the region we do not need a diff
            else{
                if(barwindowsused[homenode] == 0){
		    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, homenode, 0, globalDataWindow[homenode]);
		    barwindowsused[homenode] = 1;
	        }
                // Send the whole page
                MPI_Put((void*)((char*)startAddr + glob_addr), pagesize, MPI_BYTE, homenode, offset, pagesize, MPI_BYTE, globalDataWindow[homenode]);
            }
        }
    }

    // Make sure to sync writebacks
    for(i = 0; i < argo_get_nodes(); i++){
        if(barwindowsused[i] == 1){
            MPI_Win_unlock(i, globalDataWindow[i]); //Sync write backs
            barwindowsused[i] = 0;
        }
    }
    t2 = MPI_Wtime();
    stats.ssdtime += t2-t1;
    // Release exclusive cache access
    sem_post(&ibsem);
    pthread_mutex_unlock(&cachemutex);
}
