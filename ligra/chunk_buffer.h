#pragma once
#include <iostream>
// #include <fstream>
// #include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include "vertex.h"
#include "parallel.h"
using namespace std;

typedef uint32_t cid_t; //chunk_id
class ChunkBuffer{
private:
  size_t chunk_size;
  cid_t nchunks, nmchunks; 

  int cfd; 
  char* buff; 
  char** mchunks;
  cid_t cur_mcid;

  cid_t *cmap, *mcmap; // cid -> mcid, mcid -> cid

public:
  ChunkBuffer(char filename[], size_t _chunk_size, cid_t _nchunks, cid_t _nmchunks)
  :chunk_size(_chunk_size), nchunks(_nchunks), nmchunks(_nmchunks) {
    cfd = open(filename, O_RDONLY | O_DIRECT); //| O_NOATIME);
    if(cfd == -1)
    {
      fprintf(stdout,"Wrong open %s\n",filename);
      perror("open");
      exit(-1);
	  }

    buff = (char*)calloc(nmchunks, chunk_size);
    mchunks = (char**)calloc(nmchunks, sizeof(char*));
    for(int i = 0; i < nmchunks; i++){
      mchunks[i] = buff + i * chunk_size;
    }
    cur_mcid = 0;

    cmap = (cid_t*)calloc(nchunks, sizeof(cid_t));
    for(int i = 0; i < nchunks; i++){
      cmap[i] = nmchunks; 
    }
    mcmap = (cid_t*)calloc(nmchunks, sizeof(cid_t));
    for(int i = 0; i < nmchunks; i++){
      cmap[i] = nchunks; 
    }
  }
  ~ChunkBuffer(){
    free(mcmap);
    free(cmap);
    free(mchunks);
    free(buff);
  }

  cid_t get_cmap(cid_t cid){
    return cmap[cid];
  }
  cid_t get_mcmap(cid_t mcid){
    return mcmap[mcid];
  }

  char* get_mchunk(cid_t cid){
    if(cmap[cid] == nmchunks){ // Not in DRAM buffer
      cid_t mcid = cur_mcid;
      if(mcmap[mcid] != nchunks){
        free_chunk(mcmap[mcid], mcid);
        cmap[mcmap[mcid]] = nchunks;
      }
      load_chunk(cid, mcid);
      cmap[cid] = mcid;
      cur_mcid++;
    }
    return mchunks[cmap[cid]];
  }

  void load_chunk(cid_t cid, cid_t mcid){
    pread(cfd,mchunks[mcid],chunk_size,chunk_size*mcid);
  }
  void free_chunk(cid_t cid, cid_t mcid){
  }
}; 