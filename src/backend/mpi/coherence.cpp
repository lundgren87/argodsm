/**
 * @file
 * @brief This file implements coherence mechanisms for ArgoDSM
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#include "coherence.hpp"
#include "swdsm.h"

#define WRITE_BACK_DIRTY_ON_INVALIDATION 1

/* EXTERNAL VARIABLES FROM BACKEND */
/**@todo might declare the variables here later when we remove the old backend */
extern control_data *cacheControl;
extern unsigned long *globalSharers;
extern pthread_mutex_t cachemutex;
extern sem_t ibsem;
extern void * startAddr;
extern char * barwindowsused;
extern MPI_Win *globalDataWindow;

static const unsigned int pagesize = 4096;

//pthread_mutex_t tmmutex = PTHREAD_MUTEX_INITIALIZER;

void selective_si(void *addr, size_t size){
    //	printf("addr :%p, size:%d\n",addr,size);

    if(size == 0){
        return;
    }

    unsigned long lineindex_start =  (unsigned long)((unsigned long)(addr) - (unsigned long)(startAddr));
    unsigned long lineindex_end = lineindex_start + size - 1;
    lineindex_start/=(pagesize*CACHELINE);
    //lineindex_start*=(pagesize*CACHELINE);
    lineindex_end/=(pagesize*CACHELINE);
    //lineindex_end*=(pagesize*CACHELINE);


    /*
     *Get the offset from the start of the global address space, lets call this 'global address'
     */
//    pthread_mutex_lock(&tmmutex);  
    pthread_mutex_lock(&cachemutex);
    sem_wait(&ibsem);
    for(unsigned long p = lineindex_start; p <= lineindex_end; p++, addr+=pagesize){
        unsigned long glob_addr =  (unsigned long)((unsigned long)(addr) - (unsigned long)(startAddr));	
        glob_addr/=(pagesize*CACHELINE);
        glob_addr*=(pagesize*CACHELINE);

        unsigned long id = 1 << getID();	
        unsigned long homenode = getHomenode(glob_addr);
        unsigned long homenode_offset = getOffset(glob_addr);

        unsigned long classidx = get_classification_index(glob_addr);
        unsigned long idx = getCacheIndex(glob_addr);

#ifdef WRITE_BACK_DIRTY_ON_INVALIDATION
        argo_byte dirty = cacheControl[idx].dirty;
        int i;
        if(dirty == DIRTY){
            /**@todo should only write back this page*/
            for(i = 0; i <CACHELINE; i++){
                storepageDIFF(idx+i,glob_addr+pagesize*i);
                cacheControl[idx+i].dirty = CLEAN;
            }
            for(i = 0; i < argo_get_nodes(); i++){
                if(barwindowsused[i] == 1){
                    MPI_Win_unlock(i, globalDataWindow[i]); //Sync write backs
                    barwindowsused[i] = 0;
                }
            }

        }
#endif

        /*if(
        // node is single writer
        (globalSharers[classidx+1]==id)
        ||
        // No writer and assert that the node is a sharer
        ((globalSharers[classidx+1]==0) && ((globalSharers[classidx]&id)==id))
        ){
        //nothing - we keep the pages, SD is done in flushWB
        }*/
        //else{ //multiple writer or SO
        cacheControl[idx].dirty=CLEAN;
        cacheControl[idx].state = INVALID;
        mprotect((char*)startAddr + glob_addr, pagesize*CACHELINE, PROT_NONE);
        //}	
    }
    sem_post(&ibsem);
    pthread_mutex_unlock(&cachemutex);
    //pthread_mutex_unlock(&tmmutex);
}
