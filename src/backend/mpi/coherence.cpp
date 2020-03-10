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
extern pthread_mutex_t wbmutex;
extern sem_t ibsem;
extern void * startAddr;
extern char * barwindowsused;
extern MPI_Win *globalDataWindow;
extern MPI_Win sharerWindow;
extern argo_statistics stats;
extern argo_byte *touchedcache;
extern MPI_Comm workcomm;

static const unsigned int pagesize = 4096;

/**
 * @brief Selectively self-invalidate the memory region given by addr and size
 * @param addr The starting address of the memory region to invalidate
 * @param size The size of the memory region to invalidate
 */
void selective_si(void *addr, size_t size){
    unsigned long i, p, glob_addr, id, homenode, homenode_offset, classidx, idx;
    double t1, t2, cachet1, cachet2;

    if(size == 0){
        return;
    }

    id = 1 << getID();	
    char *address = (char*)addr;
    unsigned long lineindex_start =  (unsigned long)((unsigned long)(address) - (unsigned long)(startAddr));
    unsigned long lineindex_end = lineindex_start + size - 1;
    lineindex_start/=(pagesize*CACHELINE);
    lineindex_end/=(pagesize*CACHELINE);

	/* Lock relevant mutexes. Start statistics timekeeping */
    t1 = MPI_Wtime();
    cachet1 = MPI_Wtime();
    pthread_mutex_lock(&cachemutex);
    cachet2 = MPI_Wtime();
    stats.cachemutextime += cachet2-cachet1;
    stats.cachemutextime_ssi += cachet2-cachet1;
	pthread_mutex_lock(&wbmutex);
    sem_wait(&ibsem);

	/* Iterate over all pages to selectively invalidate */
    for(p = lineindex_start; p <= lineindex_end; p++, address+=pagesize){
        glob_addr =  (unsigned long)((unsigned long)(address) - (unsigned long)(startAddr));	
        glob_addr/=(pagesize*CACHELINE);
        glob_addr*=(pagesize*CACHELINE);

        homenode = getHomenode(glob_addr);
        homenode_offset = getOffset(glob_addr);
        classidx = get_classification_index(glob_addr);
        idx = getCacheIndex(glob_addr);

		/* If the page is dirty, downgrade it */
        argo_byte dirty = cacheControl[idx].dirty;
        if(dirty == DIRTY){
            mprotect((char*)startAddr + glob_addr, pagesize*CACHELINE, PROT_READ);
            cacheControl[idx].dirty = CLEAN;
            for(i = 0; i <CACHELINE; i++){
                storepageDIFF(idx+i,glob_addr+pagesize*i);
            }

        }

		/* Optimization to keep pages in cache if they do not
		 * need to be invalidated. */
		MPI_Win_lock(MPI_LOCK_SHARED, getID(), 0, sharerWindow);
        if(
        // node is single writer
        (globalSharers[classidx+1]==id)
        ||
        // No writer and assert that the node is a sharer
        ((globalSharers[classidx+1]==0) && ((globalSharers[classidx]&id)==id))
        ){
			MPI_Win_unlock(getID(), sharerWindow);
			touchedcache[idx]=1;
            //nothing - we keep the pages, SD is done in flushWB
        }
        else{ //multiple writer or SO, invalidate the page
			MPI_Win_unlock(getID(), sharerWindow);
            cacheControl[idx].dirty=CLEAN;
            cacheControl[idx].state = INVALID;
			touchedcache[idx]=0;
            mprotect((char*)startAddr + glob_addr, pagesize*CACHELINE, PROT_NONE);
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
    stats.ssitime += t2-t1;
	/* Poke the MPI system to force progress */
	int flag;
	MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,workcomm,&flag,MPI_STATUS_IGNORE);
	/* Release relevant mutexes */
    sem_post(&ibsem);
	pthread_mutex_unlock(&wbmutex);
    pthread_mutex_unlock(&cachemutex);
}


/**
 * @brief Selectively self-downgrade the memory region given by addr and size
 * @param addr The starting address of the memory region to downgrade
 * @param size The size of the memory region to downgrade
 */
void selective_sd(void *addr, size_t size){
    unsigned long i, p ,glob_addr, idx;
    double t1, t2, cachet1, cachet2;

    if(size == 0){
        return;
    }

    char *address = (char*)addr;
    // Get the start and end indexes of the memory region
    unsigned long lineindex_start =  (unsigned long)((unsigned long)(address) - (unsigned long)(startAddr));
    unsigned long lineindex_end = lineindex_start + size - 1;
    lineindex_start/=(pagesize*CACHELINE);
    lineindex_end/=(pagesize*CACHELINE);


	/* Lock relevant mutexes. Start statistics timekeeping */
    t1 = MPI_Wtime();
    cachet1 = MPI_Wtime();
    pthread_mutex_lock(&cachemutex);
    cachet2 = MPI_Wtime();
    stats.cachemutextime += cachet2-cachet1;
    stats.cachemutextime_ssd += cachet2-cachet1;
	pthread_mutex_lock(&wbmutex);
    sem_wait(&ibsem);

	/* Iterate over all pages to selectively downgrade */
    for(p = lineindex_start; p <= lineindex_end; p++, address+=pagesize){
        // Get the offset from the start of the global address space
        glob_addr =  (unsigned long)((unsigned long)(address) - (unsigned long)(startAddr));	
        glob_addr/=(pagesize*CACHELINE);
        glob_addr*=(pagesize*CACHELINE);

        idx = getCacheIndex(glob_addr);

        // Write back this page if it is DIRTY
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
	/* Poke the MPI system to force progress */
	int flag;
	MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,workcomm,&flag,MPI_STATUS_IGNORE);
	/* Release relevant mutexes */
	sem_post(&ibsem);
	pthread_mutex_unlock(&wbmutex);
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
    double t1, t2, cachet1, cachet2;

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

	/* Lock relevant mutexes. Start statistics timekeeping */
    t1 = MPI_Wtime();
    cachet1 = MPI_Wtime();
    pthread_mutex_lock(&cachemutex);
    cachet2 = MPI_Wtime();
    stats.cachemutextime += cachet2-cachet1;
    stats.cachemutextime_ssd += cachet2-cachet1;
	pthread_mutex_lock(&wbmutex);
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
			
            // If the size is smaller than pagesize
			if( (size<pagesize) ||
            	// OR,  it is the first page in the region it is not page aligned
                (p == lineindex_start && regionstart%pagesize != 0) || 
				// OR, it is the last page in the region and it is not page aligned
                (p == lineindex_end && regionend%pagesize != 0)) {
                // Create and send a diff for the page
                for(i = 0; i <CACHELINE; i++){
                    storepageDIFF(idx+i,glob_addr+pagesize*i);
                }
            }
            // Else, the whole page is in our region and we do not create a diff
            else{
                if(barwindowsused[homenode] == 0){
		    		MPI_Win_lock(MPI_LOCK_EXCLUSIVE, homenode, 0, globalDataWindow[homenode]);
		    		barwindowsused[homenode] = 1;
	        	}
                // Send the whole page
                MPI_Put((void*)((char*)startAddr + glob_addr), pagesize, MPI_BYTE, homenode, offset, pagesize, MPI_BYTE, globalDataWindow[homenode]);
            }
			// TODO: Send multiple consecutive pages with one MPI_Put
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
	/* Poke the MPI system to force progress */
	int flag;
	MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,workcomm,&flag,MPI_STATUS_IGNORE);
    /* Release relevant mutexes */
    sem_post(&ibsem);
	pthread_mutex_unlock(&wbmutex);
    pthread_mutex_unlock(&cachemutex);
}
