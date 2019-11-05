/**
 * @file
 * @brief       This file provices an implementation of a specialized MPI lock allowing
 *              multiple threads to access an MPI Window concurrently.
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#include "mpi_lock.hpp"


/** 
 * @brief acquire mpi_lock
 * @param lock_type MPI_LOCK_SHARED or MPI_LOCK_EXCLUSIVE
 * @param target    target node of the lock
 * @param window    MPI window to lock
 */
void mpi_lock::lock(int lock_type, int target, MPI_Win window){
    int cur;
    //int wlen[50];
    //char wname[50];
    //MPI_Win_get_name(window, wname, wlen);

    double start = MPI_Wtime();
    while(1){
        // TODO: find better solution of protecting cnt&m_hop together?
        while(unlockflag.test_and_set(std::memory_order_acquire));
        /** Add self to current lockholders */
        cur = cnt.fetch_add(1);
        unlockflag.clear(std::memory_order_release);
        /** If the lock is not held by anyone, take lock and lock MPI */
        if(!cur){
            /* Wait for previous lockholder to finish unlocking MPI. */
            while(m_act);
            int expected=0;
            // TODO: Change to exchange when done bughunting
            if(m_act.compare_exchange_strong(expected, lock_type)){
                locktime = MPI_Wtime();
                double lockstart = locktime;
                MPI_Win_lock(lock_type, target, 0, window);
                double lockend = MPI_Wtime();

                /* Indicate that lock is held */
                m_hop = 1;
                break;
            }else{
                // TODO: Fatal error for debugging, remove print later
                printf("Fatal error during lock.\n");
                EXIT_FAILURE;
            }
        } /** If the MPI lock of the same lock type is already held
           *  exit while loop to acquire lock locally */
        else if(m_hop && m_act==lock_type){
            break;
        } /** Else, locking has failed so resume while loop */
        else{
            unlock(target, window, false); //test
            while(!m_hop && m_act>0);
            // Sleep instead?
        }
    }
    /* Update time spent in lock atomically */
    double end = MPI_Wtime();
    if((end-start) > maxlocktime){
        maxlocktime = end-start;
    }
    for (double g = waittime.load(std::memory_order_acquire); !waittime.compare_exchange_strong(g, (g+end-start)););
    holders++;
}

/** 
 * @brief release mpi_lock
 * @param target    target node of the lock
 * @param window    MPI window to lock
 * @param flush     Flush MPI operations
 */
void mpi_lock::unlock(int target, MPI_Win window, bool flush){
    int cur;
    //int wlen[50];
    //char wname[50];
    //MPI_Win_get_name(window, wname, wlen);

    double start = MPI_Wtime();

    /* Assume all threads will flush */
    if(flush) {cnt_flush++; }

    /* TODO: replace  with something better */
    while(unlockflag.test_and_set(std::memory_order_acquire));

    /* Decrement lock counter */
    cur = --cnt;
    /* If we are the last lock holder, we need to unlock the MPI Window */
    if(!cur){
        if(flush) {cnt_flush--; }
        m_hop = 0;
        unlockflag.clear(std::memory_order_release);

        /* Wait for flushing to finish */
        while(cnt_flush);
        /* Unlock the MPI window on the target process */
        MPI_Win_unlock(target, window);
        unlocktime = MPI_Wtime();
        if((unlocktime-locktime) > maxtime){
            maxtime = unlocktime-locktime;
        }
        numholders += holders;
        numlocks++;
        holders = 0;
        m_act = 0;
        // wake all sleeping processes?
    } /* If we are not the last lock holder, just flush the window buffer */
    else if(flush){
        unlockflag.clear(std::memory_order_release);
        double flushstart = MPI_Wtime();
        if(m_act == MPI_LOCK_SHARED){
            MPI_Win_flush_local(target, window);
        }else{
            // TODO: Only tested to work with impi 2019.5, need to find solution
            MPI_Win_flush(target, window);
        }
        double flushend = MPI_Wtime();
        cnt_flush--;
        for (double g = flushtime.load(std::memory_order_acquire); !flushtime.compare_exchange_strong(g, (g+flushend-flushstart)););
    }else{
        unlockflag.clear(std::memory_order_release);
    }
    double end = MPI_Wtime();
    if((end-start) > maxunlocktime){
        maxunlocktime = end-start;
    }
}


/** 
 * @brief try to acquire lock
 * @param lock_type MPI_LOCK_SHARED or MPI_LOCK_EXCLUSIVE
 * @param target    target node of the lock
 * @param window    MPI window to lock
 * @return          true if successful, false otherwise
 */
bool mpi_lock::trylock(int lock_type, int target, MPI_Win window){
    int cur;

    while(unlockflag.test_and_set(std::memory_order_acquire));
    cur = cnt.fetch_add(1);
    unlockflag.clear(std::memory_order_release);
    if(!cur){
        while(m_act);
        int expected=0;
        if(!m_act.compare_exchange_weak(expected, lock_type)){
            locktime = MPI_Wtime();
            MPI_Win_lock(lock_type, target, 0, window);
            m_hop = 1;
        }else{
            printf("Fatal error during lock.\n");
            EXIT_FAILURE;
        }
    }else if(m_hop && m_act==lock_type){
        return true; 
    }else{
        unlock(target, window, false);
    }
    return false;
}

/** 
 * @brief  get timekeeping statistics
 * @return the total time spent for all threads in the mpi_lock
 */
double mpi_lock::get_waittime(){
    return waittime.load();
}

/** 
 * @brief  get timekeeping statistics
 * @return the total time spent flushing the MPI Windows
 */
double mpi_lock::get_flushtime(){
    return flushtime.load();
}

/** 
 * @brief  get timekeeping statistics
 * @return the maximum time spent holding an MPI window locked
 */
double mpi_lock::get_maxtime(){
    return maxtime;
}

/** 
 * @brief  get timekeeping statistics
 * @return the maximum time spent locking a window
 */
double mpi_lock::get_maxlocktime(){
    return maxlocktime.load();
}

/** 
 * @brief  get timekeeping statistics
 * @return the maximum time spent unlocking a window
 */
double mpi_lock::get_maxunlocktime(){
    return maxunlocktime.load();
}

/** 
 * @brief  get lock statistics
 * @return the total amount of times a lock was taken
 */
int mpi_lock::get_numholders(){
    return numholders;
}
/** 
 * @brief  get lock statistics
 * @return the total amount of MPI locks taken
 */
int mpi_lock::get_numlocks(){
    return numlocks;
}

/**
 * @brief reset the timekeeping statistics
 */
void mpi_lock::reset_stats(){
    waittime.store(0);
    flushtime.store(0);
    maxtime = 0;
    maxlocktime.store(0);
    maxunlocktime.store(0);
    numholders = 0;
    numlocks = 0;
}
