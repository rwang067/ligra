#pragma once
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <mutex>
#include <stdlib.h>
#include <sys/mman.h>
#include <asm/mman.h>
#include <cassert>
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
typedef uint32_t hot_t; //chunk_id
struct Chunk_t{
  hot_t hotness;
  uint16_t max_size;
  uint16_t cur_size;
};

class ChunkBuffer{
private:
  size_t chunk_size;
  cid_t nchunks, nmchunks; 

  int cfd; 
  void* buff; 
  char** mchunks;
  cid_t cur_mcid;
  bool is_huge_pages;

  cid_t *cmap, *mcmap; // cid -> mcid, mcid -> cid
  volatile bool* chunk_lock;

  hot_t *hotness;
  hot_t hotsum;
  
  cid_t loaded_chunk_count, freed_chunk_count;
  size_t space_waste;

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
    cur_mcid = 0;

    cmap = (cid_t*)calloc(nchunks, sizeof(cid_t));
    for(int i = 0; i < nchunks; i++){
      cmap[i] = nmchunks; 
    }
    mcmap = (cid_t*)calloc(nmchunks, sizeof(cid_t));
    for(int i = 0; i < nmchunks; i++){
      mcmap[i] = nchunks; 
    }

    cout << "buff addr = " << (void*)buff << ", buff size = " << (nmchunks * chunk_size /1024/1024) << " MB." << endl;
    // cout << "mchunks addrs = " << endl;
    // for(int i = 0; i < nmchunks; i++){
    //   cout << (void*)(mchunks[i]) << " ";
    // }
    // cout << endl;

    chunk_lock = (volatile bool*)calloc(nchunks, sizeof(volatile bool));

    hotness = (hot_t*)calloc(nmchunks, sizeof(hot_t));
    hotsum = 0;

    loaded_chunk_count = 0;
    freed_chunk_count = 0;
    space_waste = 0;

    pre_load();

    cout << "ChunkBuffer initialized " << nmchunks << " mchunks of size " << chunk_size << ", nchunks = " << nchunks << endl;
  }
  ~ChunkBuffer(){
  }

  void pre_load(){
    // cid_t mcid = 0;
    // for(cid_t cid = 0; cid < nmchunks; cid++){
    //   mcid = evict_colder(hotsum/nmchunks);
    //   load_chunk(cid, mcid);
    // }
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

      cout << "mcmap cached chunk count = " << mcached_count << endl;
      cout << "cmap cached chunk count = " << cached_count << ", hotness sum = " << hotsum << ", hotness avg = " << hotsum/nmchunks << endl;
      // cout << "loaded chunk count = " << loaded_chunk_count << ", wasted space = " << space_waste << ", avg = " << space_waste/loaded_chunk_count<< endl;
      // cout << "freed chunk count = " << freed_chunk_count << ", loaded-freed chunk count = " << (loaded_chunk_count-freed_chunk_count) << endl;
    }

    close(cfd);
    cout << "Close file, cfd = " << cfd << ".\n" << endl;
    free(mcmap);
    free(cmap);
    free(mchunks);

    if(is_huge_pages) munmap(buff, chunk_size*nmchunks);
    else free(buff);
    // free(chunk_lock);
  }

  cid_t get_cmap(cid_t cid){
    return cmap[cid];
  }
  cid_t get_mcmap(cid_t mcid){
    return mcmap[mcid];
  }

  cid_t evict_seq(){
    return __sync_fetch_and_add(&cur_mcid, 1)%nmchunks;
  }
  cid_t evict_colder(hot_t threshold){
    cid_t res_cid = __sync_fetch_and_add(&cur_mcid, 1)%nmchunks;
    while(hotness[res_cid]>threshold){
      res_cid = __sync_fetch_and_add(&cur_mcid, 1)%nmchunks;
    }
    return res_cid;
  }
  cid_t evict_coldest(){
    cid_t res_cid = cur_mcid;
    hot_t res_hot = hotness[res_cid];
    for(cid_t i = cur_mcid+1; i < nmchunks; i++){
      if(res_hot < hotness[i]){
        res_cid = i;
        res_hot = hotness[res_cid];
      }
    }
    for(cid_t i = 0; i < cur_mcid; i++){
      if(res_hot < hotness[i]){
        res_cid = i;
        res_hot = hotness[res_cid];
      }
    }
    hotness[res_cid] = 0;
    cur_mcid = res_cid;
    return res_cid;
  }

  uintE* get_nebrs_from_mchunk(cid_t cid, uint32_t coff, uint32_t d){
    char* mchunk = get_mchunk(cid);
    // update_mchunk_hot(cmap[cid], sizeof(uintE)*d);
    return (uintE*)(mchunk+coff);
  }

  char* get_mchunk(cid_t cid){
#ifdef PROFILE_EN
  int tid = getWorkersID();
  profiler.profile_get_chunk(tid);
#endif
    if(cmap[cid] == nmchunks){ // Not in DRAM buffer
      while(chunk_lock[cid]);
      if(cmap[cid] == nmchunks) { 
        while(chunk_lock[cid]);
        lock(chunk_lock[cid]);
        if(cmap[cid] == nmchunks) {  
          cid_t mcid = evict_seq();
          // cid_t mcid = evict_colder(hotsum/nmchunks);
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
  profiler.profile_load_chunk(tid);
#endif
    preada(cfd,mchunks[mcid],chunk_size,chunk_size*cid);
    cmap[cid] = mcid;
    mcmap[mcid] = cid;

    hotness[mcid] = ((Chunk_t*)(mchunks[mcid]))->hotness;
    hotsum += hotness[mcid]; // __sync_fetch_and_add(&hotsum, hotness[mcid]);

    // __sync_fetch_and_add(&loaded_chunk_count, 1);
    // uint16_t max_size = ((Chunk_t*)(mchunks[mcid]))->max_size;
    // uint16_t cur_size = ((Chunk_t*)(mchunks[mcid]))->cur_size;
    // __sync_fetch_and_add(&space_waste, max_size - cur_size);
  }
  void load_chunks(cid_t cid, cid_t mcid, cid_t count){ // read `count` chunks together
    preada(cfd,mchunks[mcid],chunk_size*count,chunk_size*cid);
    for(cid_t id = 0; id < count; id++){
      cmap[cid+id] = mcid+id;
      mcmap[mcid+id] = cid+id;
      hotness[mcid+id] = ((Chunk_t*)(mchunks[mcid+id]))->hotness;
      hotsum += hotness[mcid+id]; // __sync_fetch_and_add(&hotsum, hotness[mcid]);
    }
  }
  void free_chunk(cid_t mmcid, cid_t mcid){
    hotsum -= hotness[mcid]; // __sync_fetch_and_sub(&hotsum, hotness[mcid]);
    cmap[mmcid] = nmchunks;
    mcmap[mcid] = nchunks;

    // __sync_fetch_and_add(&freed_chunk_count, 1);
  }

  void update_mchunk_hot(cid_t mcid, hot_t h){
    hotness[mcid] += h; // __sync_fetch_and_add(&hotness[mcid], h);
    hotsum += h; // __sync_fetch_and_add(&hotsum, h);
  }
  void update_chunk_hot(cid_t cid, hot_t h){
    cid_t mcid = cmap[cid];
    if(mcid != nmchunks){
      update_mchunk_hot(mcid, h);
    }
  }
}; 