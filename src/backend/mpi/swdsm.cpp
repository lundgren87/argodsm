/**
 * @file
 * @brief This file implements the MPI-backend of ArgoDSM
 * @copyright Eta Scale AB. Licensed under the Eta Scale Open Source License. See the LICENSE file for details.
 */
#include<cstddef>

#include "signal/signal.hpp"
#include "virtual_memory/virtual_memory.hpp"
#include "swdsm.h"
#include "write_buffer.hpp"

namespace vm = argo::virtual_memory;
namespace sig = argo::signal;

/*Treads*/
/** @brief For matching threads to more sensible thread IDs */
pthread_t tid[NUM_THREADS] = {0};

/*Barrier*/
/** @brief  Locks access to part that does SD in the global barrier */
pthread_mutex_t barriermutex = PTHREAD_MUTEX_INITIALIZER;
/** @brief Thread local barrier used to first wait for all local threads in the global barrier*/
pthread_barrier_t *threadbarrier;


/*Pagecache*/
/** @brief  Size of the cache in number of pages*/
unsigned long cachesize;
/** @brief  Offset off the cache in the backing file*/
unsigned long cacheoffset;
/** @brief  Keeps state, tag and dirty bit of the cache*/
control_data * cacheControl;
/** @brief  keeps track of readers and writers*/
unsigned long *globalSharers;
/** @brief  size of pyxis directory*/
unsigned long classificationSize;
/** @brief  Tracks if a page is touched this epoch*/
argo_byte * touchedcache;
/** @brief  The local page cache*/
char* cacheData;
/** @brief Copy of the local cache to keep twinpages for later being able to DIFF stores */
char * pagecopy;
/** @brief Protects the pagecache */
pthread_mutex_t cachemutex = PTHREAD_MUTEX_INITIALIZER;

/*Writebuffer*/
/** @brief better write buffer */
WriteBuffer<unsigned long>* write_buffer;
/** @brief  Size of the writebuffer*/
size_t writebuffersize;
/** @brief writeback from writebuffer helper function */
void write_back_write_buffer();
/** @brief Lock for the writebuffer*/
pthread_mutex_t wbmutex = PTHREAD_MUTEX_INITIALIZER;

/*MPI and Comm*/
/** @brief  A copy of MPI_COMM_WORLD group to split up processes into smaller groups*/
/** @todo This can be removed now when we are only running 1 process per ArgoDSM node */
MPI_Group startgroup;
/** @brief  A group of all processes that are executing the main thread */
/** @todo This can be removed now when we are only running 1 process per ArgoDSM node */
MPI_Group workgroup;
/** @brief Communicator can be replaced with MPI_COMM_WORLD*/
MPI_Comm workcomm;
/** @brief MPI window for communicating pyxis directory*/
MPI_Win sharerWindow;
/** @brief MPI window for communicating global locks*/
MPI_Win lockWindow;
/** @brief MPI windows for reading and writing data in global address space */
MPI_Win *globalDataWindow;
/** @brief MPI data structure for sending cache control data*/
MPI_Datatype mpi_control_data;
/** @brief MPI data structure for a block containing an ArgoDSM cacheline of pages */
MPI_Datatype cacheblock;
/** @brief number of MPI processes / ArgoDSM nodes */
int numtasks;
/** @brief  rank/process ID in the MPI/ArgoDSM runtime*/
int rank;
/** @brief rank/process ID in the MPI/ArgoDSM runtime*/
int workrank;
/** @brief tracking which windows are used for reading and writing global address space*/
char * barwindowsused;
/** @brief Semaphore protecting infiniband accesses*/
/** @todo replace with a (qd?)lock */
sem_t ibsem;

/*Loading and Prefetching*/
/**
 * @brief load into cache helper function
 * @param tag aligned address to load into the cache
 * @param line cache entry index to use
 */
void load_cache_entry(unsigned long tag, unsigned long line);

/*Global lock*/
/** @brief  Local flags we spin on for the global lock*/
unsigned long * lockbuffer;
/** @brief  Protects the global lock so only 1 thread can have a global lock at a time */
sem_t globallocksem;
/** @brief  Keeps track of what local flag we should spin on per lock*/
int locknumber=0;

/*Global allocation*/
/** @brief  Keeps track of allocated memory in the global address space*/
unsigned long *allocationOffset;
/** @brief  Protects access to global allocator*/
pthread_mutex_t gmallocmutex = PTHREAD_MUTEX_INITIALIZER;

/*Common*/
/** @brief  Points to start of global address space*/
void * startAddr;
/** @brief  Points to start of global address space this process is serving */
char* globalData;
/** @brief  Size of global address space*/
unsigned long size_of_all;
/** @brief  Size of this process part of global address space*/
unsigned long size_of_chunk;
/** @brief  size of a page */
static const unsigned int pagesize = 4096;
/** @brief  Magic value for invalid cacheindices */
unsigned long GLOBAL_NULL;
/** @brief  Statistics */
argo_statistics stats;

namespace {
	/** @brief constant for invalid ArgoDSM node */
	constexpr unsigned long invalid_node = static_cast<unsigned long>(-1);
}

unsigned long isPowerOf2(unsigned long x){
  unsigned long retval =  ((x & (x - 1)) == 0); //Checks if x is power of 2 (or zero)
  return retval;
}

void flush_write_buffer(){
	double t1,t2;

	t1 = MPI_Wtime();
	pthread_mutex_lock(&wbmutex);

	/* Empty the write buffer */
	while(!write_buffer->empty()){
		unsigned long cache_index = write_buffer->pop();
		unsigned long addr = cacheControl[cache_index].tag;
		void* ptr = static_cast<char*>(startAddr) + addr;

		/* Since the page is guaranteed to be dirty, write it back */
		mprotect(ptr, pagesize*CACHELINE, PROT_READ);
		cacheControl[cache_index].dirty=CLEAN;
		for(int i=0; i < CACHELINE; i++){
			storepageDIFF(cache_index+i,pagesize*i+addr);
		}
	}

	/* Close any windows used to write back data */
	for(int i = 0; i < (unsigned long)numtasks; i++){
		if(barwindowsused[i] == 1){
			MPI_Win_unlock(i, globalDataWindow[i]);
			barwindowsused[i] = 0;
		}
	}

	pthread_mutex_unlock(&wbmutex);
	t2 = MPI_Wtime();
	stats.flushtime += t2-t1;
}

void add_to_write_buffer(unsigned long cache_index){
	pthread_mutex_lock(&wbmutex);
	
	/* If cache_index is already present in the write buffer, do nothing */
	if(write_buffer->has(cache_index)){
		pthread_mutex_unlock(&wbmutex);
		return;
	}

	/* If the buffer is full, write back "some" pages */
	if(write_buffer->size() >= (WRITE_BUFFER_PAGES/CACHELINE)){
		double t1 = MPI_Wtime();
		write_back_write_buffer();
		double t4 = MPI_Wtime();
		stats.writebacks+=CACHELINE;
		stats.writebacktime+=(t4-t1);
	}

	/* Add cache_index at the back of the write buffer */
	write_buffer->emplace_back(cache_index);
	pthread_mutex_unlock(&wbmutex);
}


int argo_get_local_tid(){
	int i;
	for(i = 0; i < NUM_THREADS; i++){
		if(pthread_equal(tid[i],pthread_self())){
			return i;
		}
	}
	return 0;
}

int argo_get_global_tid(){
	int i;
	for(i = 0; i < NUM_THREADS; i++){
		if(pthread_equal(tid[i],pthread_self())){
			return ((getID()*NUM_THREADS) + i);
		}
	}
	return 0;
}


void argo_register_thread(){
	int i;
	sem_wait(&ibsem);
	for(i = 0; i < NUM_THREADS; i++){
		if(tid[i] == 0){
			tid[i] = pthread_self();
			break;
		}
	}
	sem_post(&ibsem);
	pthread_barrier_wait(&threadbarrier[NUM_THREADS]);
}


void argo_pin_threads(){

  cpu_set_t cpuset;
  int s;
  argo_register_thread();
  sem_wait(&ibsem);
  CPU_ZERO(&cpuset);
  int pinto = argo_get_local_tid();
  CPU_SET(pinto, &cpuset);

  s = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (s != 0){
    printf("PINNING ERROR\n");
    argo_finalize();
  }
  sem_post(&ibsem);
}


//Get cacheindex
unsigned long getCacheIndex(unsigned long addr){
	unsigned long index = (addr/pagesize) % cachesize;
	return index;
}

void init_mpi_struct(void){
	//init our struct coherence unit to work in mpi.
	const int blocklen[3] = { 1,1,1};
	MPI_Aint offsets[3];
	offsets[0] = 0;  offsets[1] = sizeof(argo_byte)*1;  offsets[2] = sizeof(argo_byte)*2;

	MPI_Datatype types[3] = {MPI_BYTE,MPI_BYTE,MPI_UNSIGNED_LONG};
	MPI_Type_create_struct(3,blocklen, offsets, types, &mpi_control_data);

	MPI_Type_commit(&mpi_control_data);
}


void init_mpi_cacheblock(void){
	//init our struct coherence unit to work in mpi.
	MPI_Type_contiguous(pagesize*CACHELINE,MPI_BYTE,&cacheblock);
	MPI_Type_commit(&cacheblock);
}

/**
 * @brief align an offset into a memory region to the beginning of its size block
 * @param offset the unaligned offset
 * @param size the size of each block
 * @return the beginning of the block of size size where offset is located
 */
inline std::size_t align_backwards(std::size_t offset, std::size_t size) {
	return (offset / size) * size;
}

void handler(int sig, siginfo_t *si, void *unused){
	UNUSED_PARAM(sig);
	UNUSED_PARAM(unused);
	double cachet1, cachet2;
	double t1 = MPI_Wtime();

	unsigned long tag;
	argo_byte owner,state;
	/* compute offset in distributed memory in bytes, always positive */
	const std::size_t access_offset = static_cast<char*>(si->si_addr) - static_cast<char*>(startAddr);

	/* align access offset to cacheline */
	const std::size_t aligned_access_offset = align_backwards(access_offset, CACHELINE*pagesize);
	unsigned long classidx = get_classification_index(aligned_access_offset);

	/* compute start pointer of cacheline. char* has byte-wise arithmetics */
	char* const aligned_access_ptr = static_cast<char*>(startAddr) + aligned_access_offset;
	unsigned long startIndex = getCacheIndex(aligned_access_offset);
	unsigned long homenode = getHomenode(aligned_access_offset);
	unsigned long offset = getOffset(aligned_access_offset);
	unsigned long id = 1 << getID();
	unsigned long invid = ~id;

	cachet1 = MPI_Wtime();
	pthread_mutex_lock(&cachemutex);
	cachet2 = MPI_Wtime();
	stats.cachemutextime += cachet2-cachet1;

	/* page is local */
	if(homenode == (getID())){
		int n;
		sem_wait(&ibsem);
		unsigned long sharers;
		MPI_Win_lock(MPI_LOCK_SHARED, workrank, 0, sharerWindow);
		unsigned long prevsharer = (globalSharers[classidx])&id;
		MPI_Win_unlock(workrank, sharerWindow);
		if(prevsharer != id){
			MPI_Win_lock(MPI_LOCK_EXCLUSIVE, workrank, 0, sharerWindow);
			sharers = globalSharers[classidx];
			globalSharers[classidx] |= id;
			MPI_Win_unlock(workrank, sharerWindow);
			if(sharers != 0 && sharers != id && isPowerOf2(sharers)){
				unsigned long ownid = sharers&invid;
				unsigned long owner = workrank;
				for(n=0; n<numtasks; n++){
					if((unsigned long)(1<<n)==ownid){
						owner = n; //just get rank...
						break;
					}
				}
				if(owner==(unsigned long)workrank){
					throw "bad owner in local access";
				}
				else{
					/* update remote private holder to shared */
					MPI_Win_lock(MPI_LOCK_EXCLUSIVE, owner, 0, sharerWindow);
					MPI_Accumulate(&id, 1, MPI_LONG, owner, classidx,1,MPI_LONG,MPI_BOR,sharerWindow);
					MPI_Win_unlock(owner, sharerWindow);
				}
			}
			/* set page to permit reads and map it to the page cache */
			/** @todo Set cache offset to a variable instead of calculating it here */
			vm::map_memory(aligned_access_ptr, pagesize*CACHELINE, cacheoffset+offset, PROT_READ);

		}
		else{

			/* get current sharers/writers and then add your own id */
			MPI_Win_lock(MPI_LOCK_EXCLUSIVE, workrank, 0, sharerWindow);
			unsigned long sharers = globalSharers[classidx];
			unsigned long writers = globalSharers[classidx+1];
			globalSharers[classidx+1] |= id;
			MPI_Win_unlock(workrank, sharerWindow);

			/* remote single writer */
			if(writers != id && writers != 0 && isPowerOf2(writers&invid)){
				int n;
				for(n=0; n<numtasks; n++){
					if(((unsigned long)(1<<n))==(writers&invid)){
						owner = n; //just get rank...
						break;
					}
				}
				MPI_Win_lock(MPI_LOCK_EXCLUSIVE, owner, 0, sharerWindow);
				MPI_Accumulate(&id, 1, MPI_LONG, owner, classidx+1,1,MPI_LONG,MPI_BOR,sharerWindow);
				MPI_Win_unlock(owner, sharerWindow);
			}
			else if(writers == id || writers == 0){
				int n;
				for(n=0; n<numtasks; n++){
					if(n != workrank && ((1<<n)&sharers) != 0){
						MPI_Win_lock(MPI_LOCK_EXCLUSIVE, n, 0, sharerWindow);
						MPI_Accumulate(&id, 1, MPI_LONG, n, classidx+1,1,MPI_LONG,MPI_BOR,sharerWindow);
						MPI_Win_unlock(n, sharerWindow);
					}
				}
			}
			/* set page to permit read/write and map it to the page cache */
			vm::map_memory(aligned_access_ptr, pagesize*CACHELINE, cacheoffset+offset, PROT_READ|PROT_WRITE);

		}
		sem_post(&ibsem);
		pthread_mutex_unlock(&cachemutex);
		stats.cachemutextime_load += cachet2-cachet1;
		return;
	}

	state  = cacheControl[startIndex].state;
	tag = cacheControl[startIndex].tag;

	if(state == INVALID || (tag != aligned_access_offset && tag != GLOBAL_NULL)) {
		load_cache_entry(aligned_access_offset, (startIndex%cachesize));
		pthread_mutex_unlock(&cachemutex);
		double t2 = MPI_Wtime();
		stats.loadtime+=t2-t1;
		stats.cachemutextime_load += cachet2-cachet1;
		return;
	}

	unsigned long line = startIndex / CACHELINE;
	line *= CACHELINE;

	if(cacheControl[line].dirty == DIRTY){
		pthread_mutex_unlock(&cachemutex);
		return;
	}


	touchedcache[line] = 1;
	cacheControl[line].dirty = DIRTY;

	sem_wait(&ibsem);
	MPI_Win_lock(MPI_LOCK_SHARED, workrank, 0, sharerWindow);
	unsigned long writers = globalSharers[classidx+1];
	unsigned long sharers = globalSharers[classidx];
	MPI_Win_unlock(workrank, sharerWindow);
	/* Either already registered write - or 1 or 0 other writers already cached */
	if(writers != id && isPowerOf2(writers)){
		MPI_Win_lock(MPI_LOCK_EXCLUSIVE, workrank, 0, sharerWindow);
		globalSharers[classidx+1] |= id; //register locally
		MPI_Win_unlock(workrank, sharerWindow);

		/* register and get latest sharers / writers */
		MPI_Win_lock(MPI_LOCK_SHARED, homenode, 0, sharerWindow);
		MPI_Get_accumulate(&id, 1,MPI_LONG,&writers,1,MPI_LONG,homenode,
			classidx+1,1,MPI_LONG,MPI_BOR,sharerWindow);
		MPI_Get(&sharers,1, MPI_LONG, homenode, classidx, 1,MPI_LONG,sharerWindow);
		MPI_Win_unlock(homenode, sharerWindow);
		/* We get result of accumulation before operation so we need to account for that */
		writers |= id;
		/* Just add the (potentially) new sharers fetched to local copy */
		MPI_Win_lock(MPI_LOCK_EXCLUSIVE, workrank, 0, sharerWindow);
		globalSharers[classidx] |= sharers;
		MPI_Win_unlock(workrank, sharerWindow);

		/* check if we need to update */
		if(writers != id && writers != 0 && isPowerOf2(writers&invid)){
			int n;
			for(n=0; n<numtasks; n++){
				if(((unsigned long)(1<<n))==(writers&invid)){
					owner = n; //just get rank...
					break;
				}
			}
			MPI_Win_lock(MPI_LOCK_EXCLUSIVE, owner, 0, sharerWindow);
			MPI_Accumulate(&id, 1, MPI_LONG, owner, classidx+1,1,MPI_LONG,MPI_BOR,sharerWindow);
			MPI_Win_unlock(owner, sharerWindow);
		}
		else if(writers==id || writers==0){
			int n;
			for(n=0; n<numtasks; n++){
				if(n != workrank && ((1<<n)&sharers) != 0){
					MPI_Win_lock(MPI_LOCK_EXCLUSIVE, n, 0, sharerWindow);
					MPI_Accumulate(&id, 1, MPI_LONG, n, classidx+1,1,MPI_LONG,MPI_BOR,sharerWindow);
					MPI_Win_unlock(n, sharerWindow);
				}
			}
		}
	}
	sem_post(&ibsem);
	unsigned char * copy = (unsigned char *)(pagecopy + line*pagesize);
	memcpy(copy,aligned_access_ptr,CACHELINE*pagesize);
	add_to_write_buffer(startIndex);
	mprotect(aligned_access_ptr, pagesize*CACHELINE,PROT_WRITE|PROT_READ);
	pthread_mutex_unlock(&cachemutex);
	double t2 = MPI_Wtime();
	stats.storetime += t2-t1;
	stats.cachemutextime_store += cachet2-cachet1;
        return;
}


unsigned long getHomenode(unsigned long addr){
	unsigned long homenode = addr/size_of_chunk;
	if(homenode >=(unsigned long)numtasks){
                printf("Homenode: %lu\n - Exiting.", homenode);
		exit(EXIT_FAILURE);
	}
	return homenode;
}

unsigned long getOffset(unsigned long addr){
	//offset in local memory on remote node (homenode)
	unsigned long offset = addr - (getHomenode(addr))*size_of_chunk;
	if(offset >=size_of_chunk){
		exit(EXIT_FAILURE);
	}
	return offset;
}

void write_back_write_buffer() {
	sem_wait(&ibsem);
	int wb_size = 32;

	/* Write back wb_size elements of the write buffer */
	for(int i = 0; i < wb_size; i++){
		unsigned long cache_index = write_buffer->pop();
		unsigned long addr = cacheControl[cache_index].tag;

		mprotect((char*)startAddr+addr,CACHELINE*pagesize,PROT_READ);
		for(int j = 0; j < CACHELINE; j++){
			storepageDIFF(cache_index+j,addr+pagesize*j);
			cacheControl[cache_index+j].dirty = CLEAN;
		}
	}

	/* Close open globalDataWindows */
	for(int i = 0; i < numtasks; i++){
		if(barwindowsused[i] == 1){
			MPI_Win_unlock(i, globalDataWindow[i]);
			barwindowsused[i] = 0;
		}
	}
	sem_post(&ibsem);
}

void load_cache_entry(unsigned long loadtag, unsigned long loadline) {
	unsigned long i, j, p, n, id, invid, idx;
	unsigned long loadnode, tmpnode, startidx, endidx;
	unsigned long blocksize, lineAddr, tmpaddr, have_lock;
	void* tmpptr;

	if(loadtag >= size_of_all){ // Address out of bounds
		printf("Address %lu out of bounds.\n", loadtag);
		exit(EXIT_FAILURE);
	}
	if(loadline >= cachesize){ // Cache index out of bounds
		printf("Cache index %ld out of bounds (cachesize: %lu).\n", loadline, cachesize);
		exit(EXIT_FAILURE);
	}
	
	/** Assign node IDs */
	id = 1 << getID();
	invid = ~id;

	/** Calculate start values and store some parameters */
	blocksize = pagesize*CACHELINE;
	lineAddr = loadtag;
	lineAddr /= blocksize;
	lineAddr *= blocksize;
	startidx = loadline/CACHELINE;
	startidx *= CACHELINE;
	endidx = startidx+CACHELINE;
	loadnode = getHomenode(lineAddr);
	have_lock = 0;

	sem_wait(&ibsem);

	/** Return if cache entry is already up to date. */
	if(cacheControl[startidx].tag == lineAddr && cacheControl[startidx].state != INVALID){
		sem_post(&ibsem);
		return;
	}

	/** Adjust endidx to ensure the whole chunk to fetch is on the same node */
	p = 1;
	for(j = startidx+CACHELINE; j < startidx+LOAD_PAGES; j+=CACHELINE, p+=CACHELINE){
		tmpaddr = lineAddr + p*CACHELINE*pagesize;
		/** Increase endidx if it is within bounds and on the same node */
		if(tmpaddr < size_of_all && j < cachesize){
			tmpnode = getHomenode(tmpaddr);
			if(tmpnode == loadnode){
				endidx++;
			}
		}else{
			/** Stop when either condition is not satisfied */
			break;
		}
	}

	/** Allocate space for loading things */
	unsigned long new_sharer = 0;
	unsigned long fetch_size = endidx-startidx;
	unsigned long classidx_size = fetch_size*2;

	/** for each page to prefetch, 1 if page should be cached else 0 */
	unsigned long pages_to_load[fetch_size] = {};
	/** to store whether we were previously sharer of this page or not */
	unsigned long prevsharers[fetch_size] = {};
	/** Contains classification index for each prefetch page */
	unsigned long classidx_arr[fetch_size] = {};
	/** Used to store which classification indexes to remotely update */
	unsigned long sharerid[classidx_size] = {};
	/** Store sharers from remote node temporarily */
	unsigned long tempsharers[classidx_size] = {};
	/** temporarily store remotely fetched data */
	char * tempData = new char[fetch_size*pagesize];
	/** Used to mark pages that are already handled during remote self-downgrade */
	unsigned long handled_pages[fetch_size] = {};

	/** Write back existing cache entries if needed */
	for(idx = startidx, p = 0; idx < endidx; idx+=CACHELINE,p+=CACHELINE){
		/** Address and pointer to the data being loaded */
		tmpaddr = lineAddr + p*CACHELINE*pagesize;
		tmpptr = (char*)startAddr + tmpaddr;
		
		/** Skip updating pages that are already present and valid in the cache */
		if(cacheControl[idx].tag  == tmpaddr && cacheControl[idx].state != INVALID){
			pages_to_load[p] = 0;
			continue;
		}else{
			pages_to_load[p] = 1;
		}

		/** If wbmutex is not yet held by us, take it */
		if(!have_lock){
			if(pthread_mutex_trylock(&wbmutex) != 0){
				sem_post(&ibsem);
				pthread_mutex_lock(&wbmutex);
				sem_wait(&ibsem);
			}
			have_lock = 1;
		}

		/** If another page occupies the cache index, begin to evict it. */
		if((cacheControl[idx].tag != tmpaddr) && (cacheControl[idx].tag != GLOBAL_NULL)){
			void * oldptr = (char*)startAddr + cacheControl[idx].tag;

			/** If the page is dirty, write it back */
			if(cacheControl[idx].dirty == DIRTY){
				mprotect(oldptr,blocksize,PROT_READ);
				for(j=0; j < CACHELINE; j++){
					storepageDIFF(idx+j,pagesize*j+(cacheControl[idx].tag));
				}
			}
			/** Ensure the writeback has finished */
			for(i = 0; i < numtasks; i++){
				if(barwindowsused[i] == 1){
					MPI_Win_unlock(i, globalDataWindow[i]);
					barwindowsused[i] = 0;
				}
			} // TODO: move this out to avoid unlocking for every page?

			/* Clean up cache and protect memory */
			cacheControl[idx].state = INVALID;
			cacheControl[idx].tag = tmpaddr;
			cacheControl[idx].dirty = CLEAN;
			vm::map_memory(tmpptr, blocksize, pagesize*idx, PROT_NONE);
			mprotect(oldptr,blocksize,PROT_NONE);
		}
	}
	if(have_lock){
		pthread_mutex_unlock(&wbmutex);
		have_lock = 0;
	}
	
	/** Initialize some things */
	for(i = 0; i < fetch_size; i+=CACHELINE){
		tmpaddr = lineAddr + i*CACHELINE*pagesize;
		classidx_arr[i] = get_classification_index(tmpaddr);
	}

	/** Increase stat counter as load will be performed */
	stats.loads++;

	/** Get globalSharers info from own node and add self to it */
	MPI_Win_lock(MPI_LOCK_SHARED, workrank, 0, sharerWindow);
	for(i = 0; i < fetch_size; i+=CACHELINE){
		/** Do nothing if this page is to be skipped */
		if(pages_to_load[i] == 0) continue;
		prevsharers[i] = (globalSharers[classidx_arr[i]])&id;
		if(prevsharers[i] == 0){
			sharerid[i*2] = id;
			new_sharer = 1;
		}
	}
	MPI_Win_unlock(workrank, sharerWindow);

	/** If this node is a new sharer of at least one of the pages */
	if(new_sharer){
		/** Register ourselves in the loadnode directory */
		MPI_Win_lock(MPI_LOCK_SHARED, loadnode, 0, sharerWindow);
		MPI_Get_accumulate(sharerid, classidx_size, MPI_LONG,
				tempsharers, classidx_size, MPI_LONG,
				loadnode, classidx_arr[0], classidx_size,
				MPI_LONG, MPI_BOR, sharerWindow);
		MPI_Win_unlock(loadnode, sharerWindow);
	}

	/** Register the received remote globalSharers information locally */
	MPI_Win_lock(MPI_LOCK_EXCLUSIVE, workrank, 0, sharerWindow);
	for(i = 0; i < fetch_size; i+=CACHELINE){
		if(pages_to_load[i]){
			globalSharers[classidx_arr[i]] |= tempsharers[i*2];
			globalSharers[classidx_arr[i]] |= id;
			globalSharers[classidx_arr[i]+1] |= tempsharers[(i*2)+1];
		}
	}
	MPI_Win_unlock(workrank, sharerWindow);

	/** If any owner of a page we loaded needs to downgrade from private
	 * to shared, we need to notify it */
	for(i = 0; i < fetch_size; i+=CACHELINE){
		/** Skip pages that are not loaded or already handled */
		if(pages_to_load[i] && !handled_pages[i]){
			memset(sharerid, 0, classidx_size*sizeof(unsigned long));
			unsigned long ownid = tempsharers[i*2]&invid; // remove own bit

			/* If there is exactly one other owner that we did not already know about */
			if(isPowerOf2(ownid) && ownid != 0 && prevsharers[i] == 0){
				unsigned long owner = invalid_node; // initialize to failsafe value
				for(n=0; n<numtasks; n++) {
					if(1ul<<n==ownid) {
						owner = n; //just get rank...
						break;
					}
				}
				sharerid[i*2] = id;
				
				/** Check if any more elements need downgrading on the same node */
				for(j = i+CACHELINE; j < fetch_size; j+=CACHELINE){
					if(pages_to_load[j] && !handled_pages[j]){
						if((tempsharers[j*2]&invid) == ownid && prevsharers[j] == 0){
							sharerid[j*2] = id;
							handled_pages[j] = 1; //Ensure these are marked as completed
						}
					}
				}

				/** Downgrade all relevant pages on the owner node (P>S) */
				MPI_Win_lock(MPI_LOCK_EXCLUSIVE, owner, 0, sharerWindow);
				MPI_Accumulate(sharerid, classidx_size, MPI_LONG, owner,
						classidx_arr[0], classidx_size, MPI_LONG, MPI_BOR, sharerWindow);
				MPI_Win_unlock(owner, sharerWindow);
			}
		}
	}

	/** Finally, get the cache data and store it temporarily */
	unsigned long offset = getOffset(lineAddr);
	MPI_Win_lock(MPI_LOCK_SHARED, loadnode , 0, globalDataWindow[loadnode]);
	MPI_Get(tempData, fetch_size, cacheblock,
					loadnode, offset, fetch_size, cacheblock, globalDataWindow[loadnode]);
	MPI_Win_unlock(loadnode, globalDataWindow[loadnode]);

	/** Update the cache */
	for(idx = startidx, p = 0; idx < endidx; idx+=CACHELINE, p+=CACHELINE){
		/** Update only the pages necessary */
		if(pages_to_load[p]){
			tmpaddr = lineAddr + p*CACHELINE*pagesize;
			tmpptr = (char*)startAddr + tmpaddr;

			/** Insert the data in the node cache */
			memcpy(&cacheData[idx*blocksize], &tempData[p*blocksize], blocksize);
			
			/** If this is the first time inserting in to this index, perform vm map */
			if(cacheControl[idx].tag == GLOBAL_NULL){
				vm::map_memory(tmpptr, blocksize, pagesize*idx, PROT_READ);
				cacheControl[idx].tag = tmpaddr;
			}
			else{
				/** Else, just mprotect the region */
				mprotect(tmpptr, blocksize, PROT_READ);
			}
			touchedcache[idx] = 1;
			cacheControl[idx].state = VALID;
			cacheControl[idx].dirty=CLEAN;
		}
	}
	/** Ensure to free space occupied by tempData */
	delete[] tempData;
	sem_post(&ibsem);
}

void initmpi(){
	int ret,initialized,thread_status;
	int thread_level = MPI_THREAD_MULTIPLE;
	MPI_Initialized(&initialized);
	if (!initialized){
		ret = MPI_Init_thread(NULL,NULL,thread_level,&thread_status);
	}
	else{
		printf("MPI was already initialized before starting ArgoDSM - shutting down\n");
		exit(EXIT_FAILURE);
	}

	if (ret != MPI_SUCCESS || thread_status != thread_level) {
		printf ("MPI not able to start properly\n");
		MPI_Abort(MPI_COMM_WORLD, ret);
		exit(EXIT_FAILURE);
	}

	MPI_Comm_size(MPI_COMM_WORLD,&numtasks);
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	init_mpi_struct();
	init_mpi_cacheblock();
}

unsigned int getID(){
	return workrank;
}
unsigned int argo_get_nid(){
	return workrank;
}

unsigned int argo_get_nodes(){
	return numtasks;
}
unsigned int getThreadCount(){
	return NUM_THREADS;
}

//My sort of allocatefunction now since parmacs macros had this design
void * argo_gmalloc(unsigned long size){
	if(argo_get_nodes()==1){return malloc(size);}

	pthread_mutex_lock(&gmallocmutex);
	MPI_Barrier(workcomm);

	unsigned long roundedUp; //round up to number of pages to use.
	unsigned long currPage; //what pages has been allocated previously
	unsigned long alignment = pagesize*CACHELINE;

	roundedUp = size/(alignment);
	roundedUp = (alignment)*(roundedUp+1);

	currPage = (*allocationOffset)/(alignment);
	currPage = (alignment) *(currPage);

	if((*allocationOffset) +size > size_of_all){
		pthread_mutex_unlock(&gmallocmutex);
		return NULL;
	}

	void *ptrtmp = (char*)startAddr+*allocationOffset;
	*allocationOffset = (*allocationOffset) + roundedUp;

	if(ptrtmp == NULL){
		pthread_mutex_unlock(&gmallocmutex);
		exit(EXIT_FAILURE);
	}
	else{
		memset(ptrtmp,0,roundedUp);
	}
	swdsm_argo_barrier(1);
	pthread_mutex_unlock(&gmallocmutex);
	return ptrtmp;
}

void argo_initialize(std::size_t argo_size, std::size_t cache_size){
	int i;
	unsigned long j;
	initmpi();
	unsigned long alignment = pagesize*CACHELINE*numtasks;
	if((argo_size%alignment)>0){
		argo_size += alignment - 1;
		argo_size /= alignment;
		argo_size *= alignment;
	}

	startAddr = vm::start_address();
#ifdef ARGO_PRINT_STATISTICS
	printf("maximum virtual memory: %ld GiB\n", vm::size() >> 30);
#endif

	threadbarrier = (pthread_barrier_t *) malloc(sizeof(pthread_barrier_t)*(NUM_THREADS+1));
	for(i = 1; i <= NUM_THREADS; i++){
		pthread_barrier_init(&threadbarrier[i],NULL,i);
	}

	cachesize = 0;
	if(cache_size > argo_size) {
		cachesize += argo_size;
	} else {
		cachesize += cache_size;
	}
	cachesize += pagesize*CACHELINE;
	cachesize /= pagesize;
	cachesize /= CACHELINE;
	cachesize *= CACHELINE;

	classificationSize = 2*(argo_size/pagesize);
	writebuffersize = WRITE_BUFFER_PAGES/CACHELINE;
	write_buffer = new WriteBuffer<unsigned long>();
		assert(write_buffer->size() == 0);

	barwindowsused = (char *)malloc(numtasks*sizeof(char));
	for(i = 0; i < numtasks; i++){
		barwindowsused[i] = 0;
	}

	int *workranks = (int *) malloc(sizeof(int)*numtasks);
	int *procranks = (int *) malloc(sizeof(int)*2);
	int workindex = 0;

	for(i = 0; i < numtasks; i++){
		workranks[workindex++] = i;
		procranks[0]=i;
		procranks[1]=i+1;
	}

	MPI_Comm_group(MPI_COMM_WORLD, &startgroup);
	MPI_Group_incl(startgroup,numtasks,workranks,&workgroup);
	MPI_Comm_create(MPI_COMM_WORLD,workgroup,&workcomm);
	MPI_Group_rank(workgroup,&workrank);

	if(argo_size < pagesize*numtasks){
		argo_size = pagesize*numtasks;
	}

	alignment = CACHELINE*pagesize;
	if(argo_size % (alignment*numtasks) != 0){
		argo_size = alignment*numtasks * (1+(argo_size)/(alignment*numtasks));
	}

	//Allocate local memory for each node,
	size_of_all = argo_size; //total distr. global memory
	GLOBAL_NULL=size_of_all+1;
	size_of_chunk = argo_size/(numtasks); //part on each node
	sig::signal_handler<SIGSEGV>::install_argo_handler(&handler);

	unsigned long cacheControlSize = sizeof(control_data)*cachesize;
	unsigned long gwritersize = classificationSize*sizeof(long);

	cacheControlSize /= pagesize;
	gwritersize /= pagesize;

	cacheControlSize +=1;
	gwritersize += 1;

	cacheControlSize *= pagesize;
	gwritersize *= pagesize;

	cacheoffset = pagesize*cachesize+cacheControlSize;

	globalData = static_cast<char*>(vm::allocate_mappable(pagesize, size_of_chunk));
	cacheData = static_cast<char*>(vm::allocate_mappable(pagesize, cachesize*pagesize));
	cacheControl = static_cast<control_data*>(vm::allocate_mappable(pagesize, cacheControlSize));

	touchedcache = (argo_byte *)malloc(cachesize);
	if(touchedcache == NULL){
		printf("malloc error out of memory\n");
		exit(EXIT_FAILURE);
	}

	lockbuffer = static_cast<unsigned long*>(vm::allocate_mappable(pagesize, pagesize));
	pagecopy = static_cast<char*>(vm::allocate_mappable(pagesize, cachesize*pagesize));
	globalSharers = static_cast<unsigned long*>(vm::allocate_mappable(pagesize, gwritersize));

	char processor_name[MPI_MAX_PROCESSOR_NAME];
	int name_len;
	MPI_Get_processor_name(processor_name, &name_len);

	MPI_Barrier(MPI_COMM_WORLD);

	void* tmpcache;
	tmpcache=cacheData;
	vm::map_memory(tmpcache, pagesize*cachesize, 0, PROT_READ|PROT_WRITE);

	std::size_t current_offset = pagesize*cachesize;
	tmpcache=cacheControl;
	vm::map_memory(tmpcache, cacheControlSize, current_offset, PROT_READ|PROT_WRITE);

	current_offset += cacheControlSize;
	tmpcache=globalData;
	vm::map_memory(tmpcache, size_of_chunk, current_offset, PROT_READ|PROT_WRITE);

	current_offset += size_of_chunk;
	tmpcache=globalSharers;
	vm::map_memory(tmpcache, gwritersize, current_offset, PROT_READ|PROT_WRITE);

	current_offset += gwritersize;
	tmpcache=lockbuffer;
	vm::map_memory(tmpcache, pagesize, current_offset, PROT_READ|PROT_WRITE);

	sem_init(&ibsem,0,1);
	sem_init(&globallocksem,0,1);

	allocationOffset = (unsigned long *)calloc(1,sizeof(unsigned long));
	globalDataWindow = (MPI_Win*)malloc(sizeof(MPI_Win)*numtasks);

	for(i = 0; i < numtasks; i++){
 		MPI_Win_create(globalData, size_of_chunk*sizeof(argo_byte), 1,
									 MPI_INFO_NULL, MPI_COMM_WORLD, &globalDataWindow[i]);
	}

	MPI_Win_create(globalSharers, gwritersize, sizeof(unsigned long),
								 MPI_INFO_NULL, MPI_COMM_WORLD, &sharerWindow);
	MPI_Win_create(lockbuffer, pagesize, 1, MPI_INFO_NULL, MPI_COMM_WORLD, &lockWindow);

	memset(pagecopy, 0, cachesize*pagesize);
	memset(touchedcache, 0, cachesize);
	memset(globalData, 0, size_of_chunk*sizeof(argo_byte));
	memset(cacheData, 0, cachesize*pagesize);
	memset(lockbuffer, 0, pagesize);
	memset(globalSharers, 0, gwritersize);
	memset(cacheControl, 0, cachesize*sizeof(control_data));

	for(j=0; j<cachesize; j++){
		cacheControl[j].tag = GLOBAL_NULL;
		cacheControl[j].state = INVALID;
		cacheControl[j].dirty = CLEAN;
	}

	argo_reset_coherence(1);
}

void argo_finalize(){
	int i;
	swdsm_argo_barrier(1);
	if(getID() == 0){
		printf("ArgoDSM shutting down\n");
	}
	swdsm_argo_barrier(1);
	mprotect(startAddr,size_of_all,PROT_WRITE|PROT_READ);
	MPI_Barrier(MPI_COMM_WORLD);

	for(i=0; i <numtasks;i++){
		if(i==workrank){
			printStatistics();
		}
	}

	MPI_Barrier(MPI_COMM_WORLD);
	for(i=0; i<numtasks; i++){
		MPI_Win_free(&globalDataWindow[i]);
	}
	MPI_Win_free(&sharerWindow);
	MPI_Win_free(&lockWindow);
	MPI_Comm_free(&workcomm);
	MPI_Finalize();
	return;
}

void self_invalidation(){
	unsigned long i;
	double t1,t2;
	int flushed = 0;
	unsigned long id = 1 << getID();

	t1 = MPI_Wtime();
	for(i = 0; i < cachesize; i+=CACHELINE){
		if(touchedcache[i] != 0){
			unsigned long distrAddr = cacheControl[i].tag;
			unsigned long lineAddr = distrAddr/(CACHELINE*pagesize);
			lineAddr*=(pagesize*CACHELINE);
			unsigned long classidx = get_classification_index(lineAddr);
			argo_byte dirty = cacheControl[i].dirty;

			if(flushed == 0 && dirty == DIRTY){
				flush_write_buffer();
				flushed = 1;
			}
			MPI_Win_lock(MPI_LOCK_SHARED, workrank, 0, sharerWindow);
			if(
				 // node is single writer
				 (globalSharers[classidx+1]==id)
				 ||
				 // No writer and assert that the node is a sharer
				 ((globalSharers[classidx+1]==0) && ((globalSharers[classidx]&id)==id))
				 ){
				MPI_Win_unlock(workrank, sharerWindow);
				touchedcache[i] =1;
				/*nothing - we keep the pages, SD is done in flushWB*/
			}
			else{ //multiple writer or SO
				MPI_Win_unlock(workrank, sharerWindow);
				cacheControl[i].dirty=CLEAN;
				cacheControl[i].state = INVALID;
				touchedcache[i] =0;
				mprotect((char*)startAddr + lineAddr, pagesize*CACHELINE, PROT_NONE);
			}
		}
	}
	t2 = MPI_Wtime();
	stats.selfinvtime += (t2-t1);
}

void swdsm_argo_barrier(int n){ //BARRIER
	double time1,time2;
	double cachet1,cachet2;
	pthread_t barrierlockholder;
	time1 = MPI_Wtime();
	pthread_barrier_wait(&threadbarrier[n]);
	if(argo_get_nodes()==1){
		time2 = MPI_Wtime();
		stats.barriers++;
		stats.barriertime += (time2-time1);
		return;
	}

	if(pthread_mutex_trylock(&barriermutex) == 0){
		barrierlockholder = pthread_self();
		cachet1 = MPI_Wtime();
		pthread_mutex_lock(&cachemutex);
		cachet2 = MPI_Wtime();
		stats.cachemutextime += cachet2-cachet1;
		sem_wait(&ibsem);
		flush_write_buffer();
		MPI_Barrier(workcomm);
		self_invalidation();
		sem_post(&ibsem);
		pthread_mutex_unlock(&cachemutex);
	}

	pthread_barrier_wait(&threadbarrier[n]);
	if(pthread_equal(barrierlockholder,pthread_self())){
		pthread_mutex_unlock(&barriermutex);
		time2 = MPI_Wtime();
		stats.barriers++;
		stats.barriertime += (time2-time1);
	}
}

void argo_reset_coherence(int n){
	unsigned long j;
	stats.writebacks = 0;
	stats.stores = 0;
	memset(touchedcache, 0, cachesize);

	sem_wait(&ibsem);
	MPI_Win_lock(MPI_LOCK_EXCLUSIVE, workrank, 0, sharerWindow);
	for(j = 0; j < classificationSize; j++){
		globalSharers[j] = 0;
	}
	MPI_Win_unlock(workrank, sharerWindow);
	sem_post(&ibsem);
	swdsm_argo_barrier(n);
	mprotect(startAddr,size_of_all,PROT_NONE);
	swdsm_argo_barrier(n);
	clearStatistics();
}

void argo_acquire(){
	int flag;
	double cachet1,cachet2;
	cachet1 = MPI_Wtime();
	pthread_mutex_lock(&cachemutex);
	cachet2 = MPI_Wtime();
	stats.cachemutextime += cachet2-cachet1;
	sem_wait(&ibsem);
	self_invalidation();
	MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,workcomm,&flag,MPI_STATUS_IGNORE);
	sem_post(&ibsem);
	pthread_mutex_unlock(&cachemutex);
}


void argo_release(){
	int flag;
	double cachet1,cachet2;
	cachet1 = MPI_Wtime();
	pthread_mutex_lock(&cachemutex);
	cachet2 = MPI_Wtime();
	stats.cachemutextime += cachet2-cachet1;
	sem_wait(&ibsem);
	flush_write_buffer();
	MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,workcomm,&flag,MPI_STATUS_IGNORE);
	sem_post(&ibsem);
	pthread_mutex_unlock(&cachemutex);
}

void argo_acq_rel(){
	argo_acquire();
	argo_release();
}

double argo_wtime(){
	return MPI_Wtime();
}

void clearStatistics(){
	stats.selfinvtime = 0;
	stats.loadtime = 0;
	stats.storetime = 0;
	stats.flushtime = 0;
	stats.writebacktime = 0;
	stats.locktime=0;
	stats.barriertime = 0;
	stats.stores = 0;
	stats.writebacks = 0;
	stats.loads = 0;
	stats.barriers = 0;
	stats.locks = 0;
	stats.ssitime = 0;
	stats.ssdtime = 0;
	stats.cachemutextime = 0;
	stats.cachemutextime_load = 0;
	stats.cachemutextime_store = 0;
	stats.cachemutextime_ssi = 0;
	stats.cachemutextime_ssd = 0;
}

void storepageDIFF(unsigned long index, unsigned long addr){
	unsigned int i,j;
	int cnt = 0;
	unsigned long homenode = getHomenode(addr);
	unsigned long offset = getOffset(addr);

	char * copy = (char *)(pagecopy + index*pagesize);
	char * real = (char *)startAddr+addr;
	size_t drf_unit = sizeof(char);

	if(barwindowsused[homenode] == 0){
		MPI_Win_lock(MPI_LOCK_EXCLUSIVE, homenode, 0, globalDataWindow[homenode]);
		barwindowsused[homenode] = 1;
	}

	for(i = 0; i < pagesize; i+=drf_unit){
		int branchval;
		for(j=i; j < i+drf_unit; j++){
			branchval = real[j] != copy[j];
			if(branchval != 0){
				break;
			}
		}
		if(branchval != 0){
			cnt+=drf_unit;
		}
		else{
			if(cnt > 0){
				MPI_Put(&real[i-cnt], cnt, MPI_BYTE, homenode, offset+(i-cnt), cnt, MPI_BYTE, globalDataWindow[homenode]);
                                cnt = 0;
			}
		}
	}
	if(cnt > 0){
		MPI_Put(&real[i-cnt], cnt, MPI_BYTE, homenode, offset+(i-cnt), cnt, MPI_BYTE, globalDataWindow[homenode]);
        }
	stats.stores++;
}

void printStatistics(){
	printf("#####################STATISTICS#########################\n");
	printf("# PROCESS ID %d \n",workrank);
	printf("cachesize:%ld,CACHELINE:%ld wbsize:%ld\n",cachesize,CACHELINE,writebuffersize);
	printf("     writebacktime+=(t2-t1): %lf\n",stats.writebacktime);
	printf("# Storetime : %lf , loadtime :%lf flushtime:%lf, writebacktime: %lf\n",
			stats.storetime, stats.loadtime, stats.flushtime, stats.writebacktime);
	printf("# Barriertime : %lf, selfinvtime %lf\n",stats.barriertime, stats.selfinvtime);
	printf("stores:%lu, loads:%lu, barriers:%lu\n",stats.stores,stats.loads,stats.barriers);
	printf("Locks:%d\n",stats.locks);
	printf("SSItime:%lf, SSDtime:%lf, Cachemutextime:%lf\n", stats.ssitime, stats.ssdtime, stats.cachemutextime);
	printf("Cachemutextime - load:%lf, store:%lf\n", stats.cachemutextime_load, stats.cachemutextime_store);
	printf("Cachemutextime - ssi:%lf, ssd:%lf\n", stats.cachemutextime_ssi, stats.cachemutextime_ssd);
	printf("########################################################\n");
	printf("\n\n");
}

void *argo_get_global_base(){return startAddr;}
size_t argo_get_global_size(){return size_of_all;}

//TODO: inline and static inline give errors using selective_si outside of argodsm
unsigned long get_classification_index(uint64_t addr){
	return (2*(addr/(pagesize*CACHELINE))) % classificationSize;
}
