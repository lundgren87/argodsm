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
    int wlen[50];
    char wname[50];
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
            //printf("[%d:%lu] %s[%d]: Waiting for m_act (lock-MPI).\n", getID(), pthread_self(), wname, target);
            /* Wait for previous lockholder to finish unlocking MPI. */
            while(m_act);
            //printf("[%d:%lu] %s[%d]: Waited for m_act (lock-MPI).\n", getID(), pthread_self(), wname, target);
            int expected=0;
            // TODO: Change to exchange when done bughunting
            if(m_act.compare_exchange_strong(expected, lock_type)){
                locktime = MPI_Wtime();
                MPI_Win_lock(lock_type, target, 0, window);
                //printf("[%d:%lu] %s[%d]: MPI Locked (lock-MPI).\n", getID(), pthread_self(), wname, target);
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
    for (double g = waittime.load(); !waittime.compare_exchange_strong(g, (g+end-start)););
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

    /* Assume all threads will flush */
    if(flush) {cnt_flush++; /* printf("[%d:%lu] %s[%d]: Incremented flush (enter).\n", getID(), pthread_self(), wname, target);*/}

    /* TODO: replace  with something better */
    while(unlockflag.test_and_set(std::memory_order_acquire));

    /* Decrement lock counter */
    cur = --cnt;
    /* If we are the last lock holder, we need to unlock the MPI Window */
    if(!cur){
        if(flush) {cnt_flush--; /* printf("[%d:%lu] %s[%d]: Decremented flush (unlock).\n", getID(), pthread_self(), wname, target);*/}
        m_hop = 0;
        unlockflag.clear(std::memory_order_release);

        //printf("[%d:%lu] %s[%d]: Waiting for cnt_flush (=%d).\n", getID(), pthread_self(), wname, target, cnt_flush.load());
        /* Wait for flushing to finish */
        while(cnt_flush);
        //printf("[%d:%lu] %s[%d]: Waited for cnt_flush.\n", getID(), pthread_self(), wname, target);
        //printf("[%d:%lu] %s[%d]: Unlocking MPI (cur=%d).\n", getID(), pthread_self(), wname, target, cur);
        /* Unlock the MPI window on the target process */
        MPI_Win_unlock(target, window);
        unlocktime = MPI_Wtime();
        if((unlocktime-locktime) > maxtime){
            maxtime = unlocktime-locktime;
        }
        //printf("[%d:%lu] %s[%d]: MPI Unlocked (cur=%d).\n", getID(), pthread_self(), wname, target, cur);
        m_act = 0;
        //printf("[%d:%lu] %s[%d]: Ack reset.\n", getID(), pthread_self(), wname, target);
        // wake all sleeping processes?
    } /* If we are not the last lock holder, just flush the window buffer */
    else if(flush){
        unlockflag.clear(std::memory_order_release);
        //printf("[%d:%lu] %s[%d]: Flushing (cur=%d).\n", getID(), pthread_self(), wname, target, cur);
        double flushstart = MPI_Wtime();
        if(m_act == MPI_LOCK_SHARED){
            MPI_Win_flush_local(target, window);
        }else{
            // TODO: Only tested to work with impi 2019.5, need to find solution
            MPI_Win_flush(target, window);
        }
        double flushend = MPI_Wtime();
        //printf("[%d:%lu] %s[%d]: Flushed (cur=%d).\n", getID(), pthread_self(), wname, target, cur);
        cnt_flush--;
        for (double g = flushtime.load(); !flushtime.compare_exchange_strong(g, (g+flushend-flushstart)););
        //printf("[%d:%lu] %s[%d]: Decremented flush (flush).\n", getID(), pthread_self(), wname, target);
    }else{
        unlockflag.clear(std::memory_order_release);
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
 * @brief reset the timekeeping statistics
 */
void mpi_lock::reset_stats(){
    waittime.store(0);
    flushtime.store(0);
    maxtime = 0;
}
