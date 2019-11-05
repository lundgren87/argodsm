/**
 * @file
 * @brief Declaration of MPI lock
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */

#ifndef mpi_lock_h
#define mpi_lock_h mpi_lock_h

#include <atomic>
#include "swdsm.h"

/** @brief Provides MPI RMA epoch locking */
class mpi_lock {
private:
    /** @brief atomic lock flags */
    std::atomic<int> cnt, m_hop, m_act, cnt_flush, holders;
    
    /** @brief flag for concurrent lock and unlock calls */
    std::atomic_flag unlockflag;

    /** @brief Timekeeping */
    std::atomic<double> waittime, flushtime, maxlocktime, maxunlocktime;
    double locktime, unlocktime, maxtime, numlocks, numholders;

public:
    /**
     * @brief mpi_lock constructor 
     */
    mpi_lock()
        : cnt(0),
        m_hop(0),
        m_act(0),
        cnt_flush(0),
        waittime(0),
        flushtime(0),
        locktime(0),
        unlocktime(0),
        maxtime(0),
        maxlocktime(0),
        maxunlocktime(0),
        holders(0),
        unlockflag(ATOMIC_FLAG_INIT)
    { };

    /** 
     * @brief acquire mpi_lock
     * @param lock_type MPI_LOCK_SHARED or MPI_LOCK_EXCLUSIVE
     * @param target    target node of the lock
     * @param window    MPI window to lock
     */
    void lock(int lock_type, int target, MPI_Win window);
    
    /** 
     * @brief release mpi_lock
     * @param target    target node of the lock
     * @param window    MPI window to lock
     * @param flush     Flush MPI operations
     */
    void unlock(int target, MPI_Win window, bool flush=true); 

    /** 
     * @brief try to acquire lock
     * @param lock_type MPI_LOCK_SHARED or MPI_LOCK_EXCLUSIVE
     * @param target    target node of the lock
     * @param window    MPI window to lock
     * @return          true if successful, false otherwise
     */
    bool trylock(int lock_type, int target, MPI_Win window);


    /** 
     * @brief  get timekeeping statistics
     * @return the total time spent for all threads in the mpi_lock
     */
    double get_waittime();

    /** 
    * @brief  get timekeeping statistics
    * @return the total time spent flushing the MPI Windows
    */
    double get_flushtime();

    /** 
     * @brief  get timekeeping statistics
     * @return the maximum time spent holding an MPI window locked
     */
    double get_maxtime();

    /** 
     * @brief  get timekeeping statistics
     * @return the maximum time spent locking a window
     */
    double get_maxlocktime();

    /** 
     * @brief  get timekeeping statistics
     * @return the maximum time spent unlocking a window
     */
    double get_maxunlocktime();


    /** 
     * @brief  get lock statistics
     * @return the total amount of times a lock was taken
     */
    int get_numholders();

    /** 
     * @brief  get lock statistics
     * @return the total amount of MPI locks taken
     */
    int get_numlocks();
    
    /**
     * @brief reset the timekeeping statistics
     */
    void reset_stats();
};

#endif /* mpi_lock_h */
