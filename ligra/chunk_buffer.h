#pragma once
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <mutex>
#include <stdlib.h>
#include "parallel.h"
using namespace std;

// inline void lock(volatile bool &flag)
// {
// 	while(!__sync_bool_compare_and_swap(&flag, 0, 1)){}
// }

// inline void unlock(volatile bool &flag)
// {
// 	flag = 0;
// }

typedef uint32_t cid_t; //chunk_id
class ChunkBuffer{
private:
  size_t chunk_size;
  cid_t nchunks, nmchunks; 

  int cfd; 
  void* buff; 
  char** mchunks;
  cid_t cur_mcid;

  cid_t *cmap, *mcmap; // cid -> mcid, mcid -> cid
  // volatile bool* chunk_lock;

  uint32_t *hotness;

public:
  ChunkBuffer(char filename[], size_t _chunk_size, cid_t _nchunks, cid_t _nmchunks)
  :chunk_size(_chunk_size), nchunks(_nchunks), nmchunks(_nmchunks) {
    cfd = open(filename, O_RDONLY);// | O_DIRECT); //| O_NOATIME);
    // cfd = open(filename, O_RDONLY | O_DIRECT | O_NOATIME);
    if(cfd == -1)
    {
      fprintf(stdout,"Wrong open %s\n",filename);
      perror("open");
      exit(-1);
	  }
    cout << "Open file " << filename << ", cfd = " << cfd << endl;

    buff = (char*)calloc(nmchunks, chunk_size);
    int ret;
    /* allocate 1 KB along a 256-byte boundary */
    // ret = posix_memalign (&buf, 256, 1024);
    ret = posix_memalign (&buff, 4096, nmchunks*chunk_size);
    if (ret) {
      fprintf (stderr, "posix_memalign: %s\n", strerror (ret));
      exit(-1);
    }
    memset(buff,0,nmchunks*chunk_size);

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

    cout << "buff = " << (void*)buff << ", mchunks = " << endl;
    // for(int i = 0; i < nmchunks; i++){
    //   cout << (void*)(mchunks[i]) << " ";
    // }
    // cout << endl;

    cout << "ChunkBuffer initialized " << nmchunks << " mchunks of size " << chunk_size << ", nchunks = " << nchunks << "\n" << endl;

    // chunk_lock = (volatile bool*)calloc(nchunks, sizeof(volatile bool));

    hotness = (uint32_t*)calloc(nmchunks, sizeof(uint32_t));
  }
  ~ChunkBuffer(){
  }

  void del(){
    { // used for debug, check the correctness of cmap and mcmap

      // cout << "i<-mcmap[i]: " << endl;
      // for(int i = 0; i < nmchunks; i++){
      //   if(mcmap[i] != nchunks)
      //     cout << i << "<-" << mcmap[i] << ", ";
      // }
      // cout << endl;

      // cout << "i->cmap[i]: " << endl;
      // for(int i = 0; i < nchunks; i++){
      //   if(cmap[i] != nmchunks){
      //     cout << i << "->" << cmap[i] << ", ";
      //   }
      // }
      // cout << endl;

      int cached_count = 0;
      for(int i = 0; i < nchunks; i++){
        if(cmap[i] != nmchunks){
          cached_count++;
          if(mcmap[cmap[i]] != i)
            cout << "Error map: i cmap[i] mcmap[cmap[i]] cmap[mcmap[cmap[i]]] = " << i << ", " << cmap[i] << ", " << mcmap[cmap[i]] << ", " << cmap[mcmap[cmap[i]]] << endl;
        }
      }
      cout << "cached chunk count = " << cached_count << endl;
    }

    close(cfd);
    cout << "Close file, cfd = " << cfd << endl;
    free(mcmap);
    free(cmap);
    free(mchunks);
    free(buff);
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
  cid_t evict_cold(uint32_t threshold){
    cid_t res_cid = __sync_fetch_and_add(&cur_mcid, 1)%nmchunks;
    while(hotness[res_cid]>threshold){
      res_cid = __sync_fetch_and_add(&cur_mcid, 1)%nmchunks;
    }
    return res_cid;
  }
  cid_t evict_coldest(){
    cid_t res_cid = cur_mcid;
    uint32_t res_hot = hotness[res_cid];
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

  char* get_mchunk(cid_t cid){
    // cout << "Before get_mchunk, cid = " << cid << ", cmap[cid] = " << cmap[cid] << endl;
    if(cmap[cid] == nmchunks){ // Not in DRAM buffer
      // lock(chunk_lock[cid]);
      if(cmap[cid] == nmchunks) {  
        cid_t mcid = evict_seq();
        // cid_t mcid = evict_cold(0);
        cid_t mmcid = mcmap[mcid];
        // cout << "mcid = " << mcid << ", mcmap[mcid] = " << mcmap[mcid] << endl;
        if(mmcid != nchunks){
          // lock(chunk_lock[mmcid]);
          // free_chunk(mmcid, mcid);
          cmap[mmcid] = nmchunks;
          mmcid = nchunks;
          // unlock(chunk_lock[mmcid]);
        }
        load_chunk(cid, mcid);
        cmap[cid] = mcid;
        mcmap[mcid] = cid;
        hotness[mcid] = 0;//(uint32_t)(mchunks[mcid]);
      }
      // unlock(chunk_lock[cid]);
    }
    // cout << "After get_mchunk, cid = " << cid << ", cmap[cid] = " << cmap[cid] << ", mchunks[cmap[cid]] = " << (void*)mchunks[cmap[cid]] << "\n" << endl;
    // lock(chunk_lock[cid]);
    cid_t res_cid = cmap[cid];
    // unlock(chunk_lock[cid]);
    return mchunks[res_cid];
  }

  void load_chunk(cid_t cid, cid_t mcid){
    if(pread(cfd,mchunks[mcid],chunk_size,chunk_size*cid)==-1){
      cout << "pread wrong\n";
      cout << "cid = " << cid << ", mcid = " << mcid << endl;
      cout << "pread: fd = " << cfd << ", *buf = " << (void*)mchunks[mcid] << ", nbytes = " << chunk_size << ", offset = " << chunk_size*cid << endl;
      abort(); 
      // exit(-1);
    }
  }
  void free_chunk(cid_t cid, cid_t mcid){
  }
}; 