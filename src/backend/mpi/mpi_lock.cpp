/**
 * @file
 * @brief       This file provices an implementation of a specialized MPI lock allowing
 *              multiple threads to access an MPI Window concurrently.
 * @copyright   Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#include "mpi_lock.hpp"

#define GLOBALLOCK 0


/**
 * @brief mpi_lock constructor 
 */
mpi_lock::mpi_lock()
    /* Initialize flags */
    : cnt(0),
    m_hop(0),
    m_act(0),
    cnt_flush(0),
    unlockflag(ATOMIC_FLAG_INIT),
    /* Initialize lock timekeeping */
    locktime(0),
    maxlocktime(0),
    mpilocktime(0),
    numlockslocal(0),
    numlocksremote(0),
    /* Initialize unlock timekeeping */
    unlocktime(0),
    maxunlocktime(0),
    mpiflushtime(0),
    mpiunlocktime(0),
    /* Initialize general statkeeping */
    holdtime(0),
    maxholdtime(0),
    flagtime(0)
{ 
    pthread_spin_init(&globallock, PTHREAD_PROCESS_PRIVATE);
};

/** 
 * @brief acquire mpi_lock
 * @param lock_type MPI_LOCK_SHARED or MPI_LOCK_EXCLUSIVE
 * @param target    target node of the lock
 * @param window    MPI window to lock
 */
void mpi_lock::lock(int lock_type, int target, MPI_Win window){
    int cur;

    double lock_start = MPI_Wtime();

    if(GLOBALLOCK){
        // Take global spinlock
        pthread_spin_lock(&globallock);
        // Lock MPI
        double mpi_start = MPI_Wtime();
        acquiretime = mpi_start;
        MPI_Win_lock(lock_type, target, 0, window);
        double mpi_end = MPI_Wtime();
        mpilocktime += (mpi_end-mpi_start);
        numlocksremote++;
    }else{
        while(1){
            double flagstart = MPI_Wtime();
            // TODO: find better solution of protecting cnt&m_hop together?
            while(unlockflag.test_and_set(std::memory_order_acquire));
            /** Add self to current lockholders */
            cur = cnt.fetch_add(1);
            double flagend = MPI_Wtime();
            flagtime += flagend-flagstart;
            unlockflag.clear(std::memory_order_release);
            /** If the lock is not held by anyone, take lock and lock MPI */
            if(!cur){
                /* Wait for previous lockholder to finish unlocking MPI. */
                while(m_act);
                /* Acquire m_act with current lock type */
                if(!m_act.exchange(lock_type)){
                    /* Time and acquire MPI lock */
                    double mpi_start = MPI_Wtime();
                    acquiretime = mpi_start;
                    MPI_Win_lock(lock_type, target, 0, window);
                    double mpi_end = MPI_Wtime();
                    mpilocktime += (mpi_end-mpi_start);

                    /* Indicate that lock is held */
                    numlocksremote++;
                    m_hop = 1;
                    break;
                }else{
                    printf("Fatal error during lock.\n");
                    EXIT_FAILURE;
                }
//            } /** If we are a writer, we don't allow multiples (IMPI <2019?)*/
//            else if(lock_type == MPI_LOCK_EXCLUSIVE){
//                unlock(target, window, false);
//                while(!m_hop && m_act>0);
//                continue;
            } /** If the MPI lock of the same lock type is already held
               *  exit while loop to acquire lock locally */
            else if(m_hop && m_act==lock_type){
                numlockslocal++;
                break;
            } /** Else, locking has failed so resume while loop */
            else{
                unlock(target, window, false); //test
                while(!m_hop && m_act>0);
                // Sleep instead?
            }
        }
    }
    double lock_end = MPI_Wtime();
    /* Update max time spent in lock (NOT THREAD SAFE)*/
    if((lock_end-lock_start) > maxlocktime){
        maxlocktime = lock_end-lock_start;
    }
    /* Update time spent in lock atomically */
    for (double g = locktime.load(std::memory_order_acquire); !locktime.compare_exchange_strong(g, (g+lock_end-lock_start)););
}

/** 
 * @brief release mpi_lock
 * @param target    target node of the lock
 * @param window    MPI window to lock
 * @param flush     Flush MPI operations
 */
void mpi_lock::unlock(int target, MPI_Win window, bool flush){
    int cur;

    double unlock_start = MPI_Wtime();

    if(GLOBALLOCK){
        double mpi_start = MPI_Wtime();
        MPI_Win_unlock(target, window);
        double mpi_end = MPI_Wtime();
        mpiunlocktime += mpi_end-mpi_start;
        releasetime = mpi_end;
        holdtime += (releasetime-acquiretime);
        if((releasetime-acquiretime) > maxholdtime){
            maxholdtime = releasetime-acquiretime;
        }
        pthread_spin_unlock(&globallock);
    }else{
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
            /* Time and unlock the MPI window on the target process */
            double mpi_start = MPI_Wtime();
            MPI_Win_unlock(target, window);
            double mpi_end = MPI_Wtime();
            /* Update timers */
            mpiunlocktime += mpi_end-mpi_start;
            releasetime = mpi_end;
            holdtime += (releasetime-acquiretime);
            if((releasetime-acquiretime) > maxholdtime){
                maxholdtime = releasetime-acquiretime;
            }
            /* Indicate lock is free */
            m_act = 0;
            // wake all sleeping processes?
        } /* If we are not the last lock holder, just flush the window buffer */
        else if(flush){
            unlockflag.clear(std::memory_order_release);
            double mpi_start = MPI_Wtime();
            if(m_act == MPI_LOCK_SHARED){
                MPI_Win_flush_local(target, window);
            }else{
                // TODO: Only tested to work with impi 2019.5, need to find solution
                MPI_Win_flush(target, window);
            }
            double mpi_end = MPI_Wtime();
            cnt_flush--;
            /* Update flushtime */
            for (double g = mpiflushtime.load(std::memory_order_acquire); !mpiflushtime.compare_exchange_strong(g, (g+mpi_end-mpi_start)););
        } /* Skip flush if this lock did not write/read */
        else{
            unlockflag.clear(std::memory_order_release);
        }
    }
    double unlock_end = MPI_Wtime();
    /* Update max time spent in unlock (NOT THREAD SAFE)*/
    if((unlock_end-unlock_start) > maxunlocktime){
        maxunlocktime = unlock_end-unlock_start;
    }
    /* Update time spent in lock atomically */
    for (double g = unlocktime.load(std::memory_order_acquire); !unlocktime.compare_exchange_strong(g, (g+unlock_end-unlock_start)););
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



/*********************************************************
 * LOCK STATISTICS
 * ******************************************************/

/** 
 * @brief  get timekeeping statistics
 * @return the total time spent for all threads in the mpi_lock
 */
double mpi_lock::get_locktime(){
    return locktime.load();
}

/** 
 * @brief  get timekeeping statistics
 * @return the average time spent per lock
 */
double mpi_lock::get_avglocktime(){
    double numlocks = (double)numlockslocal.load() + (double)numlocksremote;
    if(numlocks>0){
        return locktime.load()/numlocks;
    }else{
        return 0;
    }
}

/** 
 * @brief  get timekeeping statistics
 * @return the average time spent per lock
 */
double mpi_lock::get_maxlocktime(){
    return maxlocktime;
}

/** 
 * @brief  get timekeeping statistics
 * @return the total time spent for all threads waiting for MPI_Win_lock
 */
double mpi_lock::get_mpilocktime(){
    return mpilocktime;
}

/** 
 * @brief  get timekeeping statistics
 * @return the total number of locks taken
 */
int mpi_lock::get_numlocks(){
    return numlocksremote+numlockslocal.load();
}

/*********************************************************
 * UNLOCK STATISTICS
 * ******************************************************/

/** 
 * @brief  get timekeeping statistics
 * @return the total time spent for all threads in the mpi_unlock
 */
double mpi_lock::get_unlocktime(){
    return unlocktime.load();
}

/** 
 * @brief  get timekeeping statistics
 * @return the average time spent per unlock
 */
double mpi_lock::get_avgunlocktime(){
    double numlocks = (double)numlockslocal.load() + double(numlocksremote);
    if(numlocks>0){
        return unlocktime.load()/numlocks;
    }else{
        return 0;
    }
}

/** 
 * @brief  get timekeeping statistics
 * @return the maximum time spent in mpi_unlock
 */
double mpi_lock::get_maxunlocktime(){
    return maxunlocktime;
}

/** 
 * @brief  get timekeeping statistics
 * @return the total time spent for all threads waiting for mpi_win_lock
 */
double mpi_lock::get_mpiunlocktime(){
    return mpiunlocktime;
}

/** 
 * @brief  get timekeeping statistics
 * @return the total time spent for all threads in the mpi_lock
 */
double mpi_lock::get_mpiflushtime(){
    return mpiflushtime.load();
}

/*********************************************************
 * GENERAL STATISTICS
 * ******************************************************/

/** 
 * @brief  get timekeeping statistics
 * @return the total time spent holding an mpi_lock
 */
double mpi_lock::get_holdtime(){
    return holdtime;
}

/** 
 * @brief  get timekeeping statistics
 * @return the average time spent holding an mpi_lock
 */
double mpi_lock::get_avgholdtime(){
    if(numlocksremote>0){
        return holdtime/(double)numlocksremote;
    }else{
        return 0;
    }
}

/** 
 * @brief  get timekeeping statistics
 * @return the maximum time spent holding an mpi_lock
 */
double mpi_lock::get_maxholdtime(){
    return maxholdtime;
}

/** 
 * @brief  get lock statistics
 * @return the total average lock load
 */
double mpi_lock::get_avgload(){
    double rl = (double)numlocksremote;
    if(rl>0){
        return (numlockslocal.load()+rl)/rl;
    }else{
        return 1;
    }
}

/** 
 * @brief  get lock statistics
 * @return the total time spent waiting for unlockflag
 */
double mpi_lock::get_flagtime(){
    return flagtime;
}

/**
 * @brief reset the timekeeping statistics
 */
void mpi_lock::reset_stats(){
    /* Lock stuff */
    locktime.store(0);
    maxlocktime = 0;
    mpilocktime = 0;
    numlockslocal.store(0);
    numlocksremote = 0;
    /* Unlock stuff */
    unlocktime.store(0);
    maxunlocktime = 0;
    mpiunlocktime = 0;
    mpiflushtime.store(0);
    /* Other stuff */
    holdtime = 0;
    maxholdtime = 0;
    flagtime = 0;
}
