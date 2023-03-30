#pragma once
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <mutex>
#include <stdlib.h>
#include <sys/mman.h>
#include <asm/mman.h>
#include <cassert>
#include <omp.h>
#include "parallel.h"
#include "monitor.h"

using namespace std;

#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)

inline void lock(volatile bool &flag)
{
	while(!__sync_bool_compare_and_swap(&flag, 0, 1)){}
}
inline void unlock(volatile bool &flag)
{
	flag = 0;
}

template <typename T>
void preada(int f, T * tbuf, size_t nbytes, size_t off = 0) {
    size_t nread = 0;
    T * buf = (T*)tbuf;
    while(nread<nbytes) {
        ssize_t a = pread(f, buf, nbytes - nread, off + nread);
        if (a == (-1)) {
            std::cout << "Error, could not read: " << strerror(errno) << "; file-desc: " << f << std::endl;
            std::cout << "Pread arguments: " << f << " tbuf: " << tbuf << " nbytes: " << nbytes << " off: " << off << std::endl;
            exit(-1);
        }
        buf += a/sizeof(T);
        nread += a;
    }
    assert(nread <= nbytes);
}

typedef uint32_t cid_t; //chunk_id
typedef uint32_t hot_t; //hot value
typedef uint32_t tid_t; //thread_id
struct Chunk_t{
  hot_t hotness;
  uint16_t max_size;
  uint16_t cur_size;
};
struct thd_mchunk_t{
  cid_t cur_mcid;
  hot_t hotsum;
  cid_t num_get_chunk;
  cid_t num_load_chunk;
  cid_t num_free_chunk;
  size_t space_waste;
};

class ChunkBuffer{
private:
  size_t chunk_size;
  cid_t nchunks, nmchunks, nmchunks_per_thd; 

  int cfd; 
  void *buff; 
  char **mchunks;

  cid_t *cmap, *mcmap; // cid -> mcid, mcid -> cid
  volatile bool *chunk_lock;
  hot_t *hotness;

  uint32_t nthreads;
  thd_mchunk_t* thd_mchunk_states;

  bool is_huge_pages;

public:
  ChunkBuffer(const char* filename, size_t _chunk_size, cid_t _nchunks, cid_t _nmchunks)
  :chunk_size(_chunk_size), nchunks(_nchunks), nmchunks(_nmchunks) {
    // cfd = open(filename, O_RDONLY);// | O_DIRECT); //| O_NOATIME);
    cfd = open(filename, O_RDONLY | O_DIRECT | O_NOATIME);
    if(cfd == -1)
    {
      fprintf(stdout,"Wrong open %s\n",filename);
      perror("open");
      exit(-1);
	  }
    cout << "Open file " << filename << ", cfd = " << cfd << endl;

    buff = 0;
    if(chunk_size >= 2097152){
      is_huge_pages = 1;
      buff = (char *)mmap(NULL, nmchunks*chunk_size, PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
      if(buff == MAP_FAILED) {	
        perror("mmap");
        buff = 0;
      }
    } 
    if(buff == 0) {
      is_huge_pages = 0;
      buff = (char*)calloc(nmchunks, chunk_size);
    }

    int ret;
    /* allocate 1 KB along a 256-byte boundary */
    // ret = posix_memalign (&buf, 256, 1024);
    ret = posix_memalign (&buff, chunk_size, nmchunks*chunk_size);
    if (ret) {
      fprintf (stderr, "posix_memalign: %s\n", strerror (ret));
      exit(-1);
    }
    memset(buff, 0, nmchunks*chunk_size);

    mchunks = (char**)calloc(nmchunks, sizeof(char*));
    for(int i = 0; i < nmchunks; i++){
      mchunks[i] = (char*)buff + i * chunk_size;
    }
    mcmap = (cid_t*)calloc(nmchunks, sizeof(cid_t));
    for(int i = 0; i < nmchunks; i++){
      mcmap[i] = nchunks; 
    }
    hotness = (hot_t*)calloc(nmchunks, sizeof(hot_t));

    cmap = (cid_t*)calloc(nchunks, sizeof(cid_t));
    for(int i = 0; i < nchunks; i++){
      cmap[i] = nmchunks; 
    }
    chunk_lock = (volatile bool*)calloc(nchunks, sizeof(volatile bool));

    cout << "buff addr = " << (void*)buff << ", buff size = " << (nmchunks * chunk_size /1024/1024) << " MB." << endl;
    // cout << "mchunks addrs = " << endl;
    // for(int i = 0; i < nmchunks; i++){
    //   cout << (void*)(mchunks[i]) << " ";
    // }
    // cout << endl;

    nthreads = 96;
    nmchunks_per_thd = nmchunks/nthreads;
    thd_mchunk_states = (thd_mchunk_t*)calloc(nthreads, sizeof(thd_mchunk_t));
    for(int tid = 0; tid < nthreads; tid++){
      register thd_mchunk_t *thd_mchunk = &(thd_mchunk_states[tid]);
      thd_mchunk->cur_mcid = 0;
      thd_mchunk->hotsum = 0;
      thd_mchunk->num_get_chunk = 0;
      thd_mchunk->num_load_chunk = 0;
      thd_mchunk->num_free_chunk = 0;
      thd_mchunk->space_waste = 0;
    }

    pre_load();

    cout << "ChunkBuffer initialized " << nmchunks << " mchunks of size " << chunk_size << ", nchunks = " << nchunks << endl;
  }
  ~ChunkBuffer(){
  }

  void pre_load(){
    load_chunks(0,0,nmchunks);
    cout << "ChunkBuffer pre_loaded the former " << nmchunks << " chunks." << endl;
  }

  void del(){
    { // used for debug, check the correctness of cmap and mcmap
      int mcached_count = 0;
      for(int i = 0; i < nmchunks; i++){
        if(mcmap[i] != nchunks){
          mcached_count++;
          if(cmap[mcmap[i]] != i)
            cout << "Error cmap of chunksize_" << chunk_size << " : i mcmap[i] cmap[mcmap[i]] mcmap[cmap[mcmap[i]]] = " << i << ", " << mcmap[i] << ", " << cmap[mcmap[i]] << ", " << mcmap[cmap[mcmap[i]]] << endl;
        }
      }

      int cached_count = 0;
      for(int i = 0; i < nchunks; i++){
        if(cmap[i] != nmchunks){
          cached_count++;
          if(mcmap[cmap[i]] != i)
            cout << "Error mcmap of chunksize_" << chunk_size << " : i cmap[i] mcmap[cmap[i]] cmap[mcmap[cmap[i]]] = " << i << ", " << cmap[i] << ", " << mcmap[cmap[i]] << ", " << cmap[mcmap[cmap[i]]] << endl;
        }
      }

      hot_t hotsum = 0;
      cid_t gotten_chunk_count = 0, loaded_chunk_count = 0, freed_chunk_count = 0, space_waste_sum = 0;
      for(int tid = 0; tid < nthreads; tid++){
        hotsum += thd_mchunk_states[tid].hotsum;
        gotten_chunk_count += thd_mchunk_states[tid].num_get_chunk;
        loaded_chunk_count += thd_mchunk_states[tid].num_load_chunk;
        freed_chunk_count += thd_mchunk_states[tid].num_free_chunk;
        space_waste_sum += thd_mchunk_states[tid].space_waste;
      }

      cout << "mcmap cached chunk count = " << mcached_count << endl;
      cout << "cmap cached chunk count = " << cached_count << ", hotness sum = " << hotsum << ", hotness avg = " << hotsum/nmchunks << endl;
      cout << "Loaded chunk count = " << loaded_chunk_count << ", wasted space = " << space_waste_sum << ", avg = " << space_waste_sum/loaded_chunk_count<< endl;
      cout << "Freed chunk count = " << freed_chunk_count << ", loaded-freed chunk count = " << (loaded_chunk_count-freed_chunk_count) << endl;
      cout << "Gotten chunk count = " << gotten_chunk_count << ", Cache hit rate = " << (double)((gotten_chunk_count-loaded_chunk_count)*1.0/gotten_chunk_count) << endl;
    }

    free(thd_mchunk_states);
    free(mcmap);
    free(cmap);
    free(mchunks);
    if(is_huge_pages) munmap(buff, chunk_size*nmchunks);
    else free(buff);
    // free(chunk_lock);

    close(cfd);
    cout << "Close file, cfd = " << cfd << ".\n" << endl;
  }

  cid_t get_cmap(cid_t cid){
    return cmap[cid];
  }
  cid_t get_mcmap(cid_t mcid){
    return mcmap[mcid];
  }

  cid_t fetch_and_add_cur_mcid(tid_t tid){
    return tid*nmchunks_per_thd + (thd_mchunk_states[tid].cur_mcid++)%nmchunks_per_thd;
  }
  cid_t evict_seq(){
    int tid = omp_get_thread_num();
    return fetch_and_add_cur_mcid(tid);
  }
  cid_t evict_rand(){
    int tid = omp_get_thread_num();
    return tid*nmchunks_per_thd + rand()%nmchunks_per_thd;
  }
  cid_t evict_colder(hot_t threshold){
    int tid = omp_get_thread_num();
    // cid_t res_cid = __sync_fetch_and_add(&cur_mcid, 1)%nmchunks;
    cid_t res_cid = fetch_and_add_cur_mcid(tid);
    while(hotness[res_cid]>threshold){
      res_cid = fetch_and_add_cur_mcid(tid);
    }
    return res_cid;
  }
  cid_t evict_colder_decay(){
    int tid = omp_get_thread_num();
    hot_t *thd_hotsum = &(thd_mchunk_states[tid].hotsum);
    hot_t threshold = (*thd_hotsum)/nmchunks_per_thd;
    cid_t res_cid = fetch_and_add_cur_mcid(tid);
    while(hotness[res_cid]>threshold){
      hot_t decay = hotness[res_cid] * 0.5;
      hotness[res_cid] -= decay;
      (*thd_hotsum) -= decay;
      threshold = (*thd_hotsum)/nmchunks_per_thd;
      // threshold -= decay/nmchunks_per_thd;
      res_cid = fetch_and_add_cur_mcid(tid);
    }
    return res_cid;
  }

  uintE* get_nebrs_from_mchunk(cid_t cid, uint32_t coff, uint32_t d){
    char* mchunk = get_mchunk(cid);
    add_mchunk_hot(cmap[cid], sizeof(uintE)*d);
    return (uintE*)(mchunk+coff);
  }

  char* get_mchunk(cid_t cid){
#ifdef PROFILE_EN
  int tid = getWorkersID();
  thd_mchunk_states[tid].num_get_chunk++;
  // profiler.profile_get_chunk(tid);
#endif
    if(cmap[cid] == nmchunks){ // Not in DRAM buffer
      while(chunk_lock[cid]);
      if(cmap[cid] == nmchunks) { 
        while(chunk_lock[cid]);
        lock(chunk_lock[cid]);
        if(cmap[cid] == nmchunks) {  
          // cid_t mcid = evict_seq();
          // cid_t mcid = evict_rand();
          // cid_t mcid = evict_colder(hotsum/nmchunks*4);
          cid_t mcid = evict_colder_decay();
          cid_t mmcid = mcmap[mcid];
          if(mmcid != nchunks){
            free_chunk(mmcid, mcid);
          }
          load_chunk(cid, mcid);
        }
        unlock(chunk_lock[cid]);
      }
    }
    return mchunks[cmap[cid]];
  }

  void load_chunk(cid_t cid, cid_t mcid){
#ifdef PROFILE_EN
  int tid = getWorkersID();
  thd_mchunk_states[tid].num_load_chunk++;
  // profiler.profile_load_chunk(tid);
#endif
    preada(cfd,mchunks[mcid],chunk_size,chunk_size*cid);
    cmap[cid] = mcid;
    mcmap[mcid] = cid;
    assign_mchunk_hot(mcid, ((Chunk_t*)(mchunks[mcid]))->hotness);
  }
  void load_chunks(cid_t cid, cid_t mcid, cid_t count){ // read `count` chunks together
#ifdef PROFILE_EN
  int tid = getWorkersID();
  thd_mchunk_states[tid].num_load_chunk += count;
  // profiler.profile_load_chunk(tid, count);
#endif
    preada(cfd,mchunks[mcid],chunk_size*count,chunk_size*cid);
    for(cid_t id = 0; id < count; id++){
      cmap[cid+id] = mcid+id;
      mcmap[mcid+id] = cid+id;
       assign_mchunk_hot(mcid, ((Chunk_t*)(mchunks[mcid+id]))->hotness);
    }
  }
  void free_chunk(cid_t mmcid, cid_t mcid){
#ifdef PROFILE_EN
  int tid = getWorkersID();
  thd_mchunk_states[tid].num_free_chunk++;
#endif
    clear_mchunk_hot(mcid);
    cmap[mmcid] = nmchunks;
    mcmap[mcid] = nchunks;
  }

  void assign_mchunk_hot(cid_t mcid, hot_t h){
    int tid = omp_get_thread_num();
    hotness[mcid] = h;
    thd_mchunk_states[tid].hotsum += h;
  }
  void clear_mchunk_hot(cid_t mcid){
    int tid = omp_get_thread_num();
    thd_mchunk_states[tid].hotsum -= hotness[mcid];
    hotness[mcid] = 0;
  }
  void add_mchunk_hot(cid_t mcid, hot_t h){
    int tid = omp_get_thread_num();
    hotness[mcid] += h; // __sync_fetch_and_add(&hotness[mcid], h);
    thd_mchunk_states[tid].hotsum += h; // __sync_fetch_and_add(&hotsum, h);
  }
  void add_chunk_hot(cid_t cid, hot_t h){
    cid_t mcid = cmap[cid];
    if(mcid != nmchunks){
      add_mchunk_hot(mcid, h);
    }
  }
}; 