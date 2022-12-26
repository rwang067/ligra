#pragma once
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
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
    cfd = open(filename, O_RDONLY);// | O_DIRECT); //| O_NOATIME);
    if(cfd == -1)
    {
      fprintf(stdout,"Wrong open %s\n",filename);
      perror("open");
      exit(-1);
	  }
    cout << "Open file " << filename << ", cfd = " << cfd << endl;

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
      mcmap[i] = nchunks; 
    }

    cout << "buff = " << (void*)buff << ", mchunks = " << endl;
    for(int i = 0; i < nmchunks; i++){
      cout << (void*)(mchunks[i]) << " ";
    }
    cout << endl;

    cout << "ChunkBuffer initialized " << nmchunks << " mchunks of size " << chunk_size << ", nchunks = " << nchunks << "\n" << endl;
  }
  ~ChunkBuffer(){
  }

  void del(){
    close(cfd);
    cout << "Close file, cfd = " << cfd << endl;
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
    // cout << "Before get_mchunk, cid = " << cid << ", cmap[cid] = " << cmap[cid] << endl;
    if(cmap[cid] == nmchunks){ // Not in DRAM buffer
      cid_t mcid = cur_mcid;
      // cout << "mcid = " << mcid << ", mcmap[mcid] = " << mcmap[mcid] << endl;
      if(mcmap[mcid] != nchunks){
        free_chunk(mcmap[mcid], mcid);
        cmap[mcmap[mcid]] = nmchunks;
        mcmap[mcid] = nchunks;
      }
      load_chunk(cid, mcid);
      cmap[cid] = mcid;
      cur_mcid = (cur_mcid + 1)%nmchunks;
    }
    // cout << "After get_mchunk, cid = " << cid << ", cmap[cid] = " << cmap[cid] << ", mchunks[cmap[cid]] = " << (void*)mchunks[cmap[cid]] << "\n" << endl;
    return mchunks[cmap[cid]];
  }

  void load_chunk(cid_t cid, cid_t mcid){
    if(pread(cfd,mchunks[mcid],chunk_size,chunk_size*cid)==-1){
      cout << "pread: fd = " << cfd << ", *buf = " << (void*)mchunks[mcid] << ", nbytes = " << chunk_size << ", offset = " << chunk_size*cid << endl;
      cout << "pread wrong\n"; abort(); 
      exit(-1);
    }
  }
  void free_chunk(cid_t cid, cid_t mcid){
  }
}; 