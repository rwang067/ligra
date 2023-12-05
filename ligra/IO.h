// This code is part of the project "Ligra: A Lightweight Graph Processing
// Framework for Shared Memory", presented at Principles and Practice of
// Parallel Programming, 2013.
// Copyright (c) 2013 Julian Shun and Guy Blelloch
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights (to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#pragma once
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <cmath>
#include <sys/mman.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "parallel.h"
#include "blockRadixSort.h"
#include "quickSort.h"
#include "utils.h"
#include "graph.h"
#include "gettime.h"
using namespace std;

typedef pair<uintE,uintE> intPair;
typedef pair<uintE, pair<uintE,intE> > intTriple;

template <class E>
struct pairFirstCmp {
  bool operator() (pair<uintE,E> a, pair<uintE,E> b) {
    return a.first < b.first; }
};

template <class E>
struct getFirst {uintE operator() (pair<uintE,E> a) {return a.first;} };

template <class IntType>
struct pairBothCmp {
  bool operator() (pair<uintE,IntType> a, pair<uintE,IntType> b) {
    if (a.first != b.first) return a.first < b.first;
    return a.second < b.second;
  }
};

// A structure that keeps a sequence of strings all allocated from
// the same block of memory
struct words {
  long n; // total number of characters
  char* Chars;  // array storing all strings
  long m; // number of substrings
  char** Strings; // pointers to strings (all should be null terminated)
  words() {}
words(char* C, long nn, char** S, long mm)
: Chars(C), n(nn), Strings(S), m(mm) {}
  void del() {free(Chars); free(Strings);}
};

inline bool isSpace(char c) {
  switch (c)  {
  case '\r':
  case '\t':
  case '\n':
  case 0:
  case ' ' : return true;
  default : return false;
  }
}

_seq<char> mmapStringFromFile(const char *filename) {
  struct stat sb;
  int fd = open(filename, O_RDONLY);
  if (fd == -1) {
    perror("open");
    exit(-1);
  }
  if (fstat(fd, &sb) == -1) {
    perror("fstat");
    exit(-1);
  }
  if (!S_ISREG (sb.st_mode)) {
    perror("not a file\n");
    exit(-1);
  }
  char *p = static_cast<char*>(mmap(0, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
  if (p == MAP_FAILED) {
    perror("mmap");
    exit(-1);
  }
  if (close(fd) == -1) {
    perror("close");
    exit(-1);
  }
  size_t n = sb.st_size;
//  char *bytes = newA(char, n);
//  parallel_for(size_t i=0; i<n; i++) {
//    bytes[i] = p[i];
//  }
//  if (munmap(p, sb.st_size) == -1) {
//    perror("munmap");
//    exit(-1);
//  }
//  cout << "mmapped" << endl;
//  free(bytes);
//  exit(0);
  return _seq<char>(p, n);
}

_seq<char> readStringFromFile(char *fileName) {
  ifstream file (fileName, ios::in | ios::binary | ios::ate);
  if (!file.is_open()) {
    std::cout << "Unable to open file: " << fileName << std::endl;
    abort();
  }
  long end = file.tellg();
  file.seekg (0, ios::beg);
  long n = end - file.tellg();
  char* bytes = newA(char,n+1);
  file.read (bytes,n);
  file.close();
  return _seq<char>(bytes,n);
}

// parallel code for converting a string to words
words stringToWords(char *Str, long n) {
  {parallel_for (long i=0; i < n; i++)
      if (isSpace(Str[i])) Str[i] = 0; }

  // mark start of words
  bool *FL = newA(bool,n);
  FL[0] = Str[0];
  {parallel_for (long i=1; i < n; i++) FL[i] = Str[i] && !Str[i-1];}

  // offset for each start of word
  _seq<long> Off = sequence::packIndex<long>(FL, n);
  long m = Off.n;
  long *offsets = Off.A;

  // pointer to each start of word
  char **SA = newA(char*, m);
  {parallel_for (long j=0; j < m; j++) SA[j] = Str+offsets[j];}

  free(offsets); free(FL);
  return words(Str,n,SA,m);
}

template <class vertex>
graph<vertex> readGraphFromFile(char* fname, bool isSymmetric, bool mmap) {
  words W;
  if (mmap) {
    _seq<char> S = mmapStringFromFile(fname);
    char *bytes = newA(char, S.n);
    // Cannot mutate the graph unless we copy.
    parallel_for(size_t i=0; i<S.n; i++) {
      bytes[i] = S.A[i];
    }
    if (munmap(S.A, S.n) == -1) {
      perror("munmap");
      exit(-1);
    }
    S.A = bytes;
    W = stringToWords(S.A, S.n);
  } else {
    _seq<char> S = readStringFromFile(fname);
    W = stringToWords(S.A, S.n);
  }
#ifndef WEIGHTED
  if (W.Strings[0] != (string) "AdjacencyGraph") {
#else
  if (W.Strings[0] != (string) "WeightedAdjacencyGraph") {
#endif
    cout << "Bad input file" << endl;
    abort();
  }

  long len = W.m -1;
  long n = atol(W.Strings[1]);
  long m = atol(W.Strings[2]);
#ifndef WEIGHTED
  if (len != n + m + 2) {
#else
  if (len != n + 2*m + 2) {
#endif
    cout << "Bad input file" << endl;
    abort();
  }

  uintT* offsets = newA(uintT,n);
#ifndef WEIGHTED
  uintE* edges = newA(uintE,m);
#else
  intE* edges = newA(intE,2*m);
#endif

  {parallel_for(long i=0; i < n; i++) offsets[i] = atol(W.Strings[i + 3]);}
  {parallel_for(long i=0; i<m; i++) {
#ifndef WEIGHTED
      edges[i] = atol(W.Strings[i+n+3]);
#else
      edges[2*i] = atol(W.Strings[i+n+3]);
      edges[2*i+1] = atol(W.Strings[i+n+m+3]);
#endif
    }}
  //W.del(); // to deal with performance bug in malloc

  vertex* v = newA(vertex,n);

  {parallel_for (uintT i=0; i < n; i++) {
    uintT o = offsets[i];
    uintT l = ((i == n-1) ? m : offsets[i+1])-offsets[i];
    v[i].setOutDegree(l);
#ifndef WEIGHTED
    v[i].setOutNeighbors(edges+o);
#else
    v[i].setOutNeighbors(edges+2*o);
#endif
    }}

  if(!isSymmetric) {
    uintT* tOffsets = newA(uintT,n);
    {parallel_for(long i=0;i<n;i++) tOffsets[i] = INT_T_MAX;}
#ifndef WEIGHTED
    intPair* temp = newA(intPair,m);
#else
    intTriple* temp = newA(intTriple,m);
#endif
    {parallel_for(long i=0;i<n;i++){
      uintT o = offsets[i];
      for(uintT j=0;j<v[i].getOutDegree();j++){
#ifndef WEIGHTED
	temp[o+j] = make_pair(v[i].getOutNeighbor(j),i);
#else
	temp[o+j] = make_pair(v[i].getOutNeighbor(j),make_pair(i,v[i].getOutWeight(j)));
#endif
      }
      }}
    free(offsets);

#ifndef WEIGHTED
#ifndef LOWMEM
    intSort::iSort(temp,m,n+1,getFirst<uintE>());
#else
    quickSort(temp,m,pairFirstCmp<uintE>());
#endif
#else
#ifndef LOWMEM
    intSort::iSort(temp,m,n+1,getFirst<intPair>());
#else
    quickSort(temp,m,pairFirstCmp<intPair>());
#endif
#endif

    tOffsets[temp[0].first] = 0;
#ifndef WEIGHTED
    uintE* inEdges = newA(uintE,m);
    inEdges[0] = temp[0].second;
#else
    intE* inEdges = newA(intE,2*m);
    inEdges[0] = temp[0].second.first;
    inEdges[1] = temp[0].second.second;
#endif
    {parallel_for(long i=1;i<m;i++) {
#ifndef WEIGHTED
      inEdges[i] = temp[i].second;
#else
      inEdges[2*i] = temp[i].second.first;
      inEdges[2*i+1] = temp[i].second.second;
#endif
      if(temp[i].first != temp[i-1].first) {
	tOffsets[temp[i].first] = i;
      }
      }}

    free(temp);

    //fill in offsets of degree 0 vertices by taking closest non-zero
    //offset to the right
    sequence::scanIBack(tOffsets,tOffsets,n,minF<uintT>(),(uintT)m);

    {parallel_for(long i=0;i<n;i++){
      uintT o = tOffsets[i];
      uintT l = ((i == n-1) ? m : tOffsets[i+1])-tOffsets[i];
      v[i].setInDegree(l);
#ifndef WEIGHTED
      v[i].setInNeighbors(inEdges+o);
#else
      v[i].setInNeighbors(inEdges+2*o);
#endif
      }}

    free(tOffsets);
    Uncompressed_Mem<vertex>* mem = new Uncompressed_Mem<vertex>(v,n,m,edges,inEdges);
    return graph<vertex>(v,n,m,mem);
  }
  else {
    free(offsets);
    Uncompressed_Mem<vertex>* mem = new Uncompressed_Mem<vertex>(v,n,m,edges);
    return graph<vertex>(v,n,m,mem);
  }
}

template <class vertex>
graph<vertex> readGraphFromBinary(char* iFile, bool isSymmetric) {
  char* config = (char*) ".config";
  char* adj = (char*) ".adj";
  char* idx = (char*) ".idx";
  char configFile[strlen(iFile)+strlen(config)+1];
  char adjFile[strlen(iFile)+strlen(adj)+1];
  char idxFile[strlen(iFile)+strlen(idx)+1];
  *configFile = *adjFile = *idxFile = '\0';
  strcat(configFile,iFile);
  strcat(adjFile,iFile);
  strcat(idxFile,iFile);
  strcat(configFile,config);
  strcat(adjFile,adj);
  strcat(idxFile,idx);

  ifstream in(configFile, ifstream::in);
  long n;
  in >> n;
  in.close();

  ifstream in2(adjFile,ifstream::in | ios::binary); //stored as uints
  in2.seekg(0, ios::end);
  long size = in2.tellg();
  in2.seekg(0);
#ifdef WEIGHTED
  long m = size/(2*sizeof(uint));
#else
  long m = size/sizeof(uint);
#endif
  char* s = (char *) malloc(size);
  in2.read(s,size);
  in2.close();
  uintE* edges = (uintE*) s;

  ifstream in3(idxFile,ifstream::in | ios::binary); //stored as longs
  in3.seekg(0, ios::end);
  size = in3.tellg();
  in3.seekg(0);
  if(n+1 != size/sizeof(intT)) { 
    cout << n << " " << size << " " << sizeof(intT) << " " << size/sizeof(intT) << " " << size/8 << std::endl;
    cout << "File size wrong\n"; abort(); 
  }

  char* t = (char *) malloc(size);
  in3.read(t,size);
  in3.close();
  uintT* offsets = (uintT*) t;

  vertex* v = newA(vertex,n);
#ifdef WEIGHTED
  intE* edgesAndWeights = newA(intE,2*m);
  {parallel_for(long i=0;i<m;i++) {
    edgesAndWeights[2*i] = edges[i];
    edgesAndWeights[2*i+1] = edges[i+m];
    }}
  //free(edges);
#endif
  {parallel_for(long i=0;i<n;i++) {
    uintT o = offsets[i];
    uintT l = offsets[i+1]-offsets[i];
    // uintT l = ((i==n-1) ? m : offsets[i+1])-offsets[i];
      v[i].setOutDegree(l);
#ifndef WEIGHTED
      v[i].setOutNeighbors((uintE*)edges+o);
#else
      v[i].setOutNeighbors(edgesAndWeights+2*o);
#endif
    }}

  if(!isSymmetric) {
    uintT* tOffsets = newA(uintT,n);
    {parallel_for(long i=0;i<n;i++) tOffsets[i] = INT_T_MAX;}
#ifndef WEIGHTED
    intPair* temp = newA(intPair,m);
#else
    intTriple* temp = newA(intTriple,m);
#endif
    {parallel_for(intT i=0;i<n;i++){
      uintT o = offsets[i];
      for(uintT j=0;j<v[i].getOutDegree();j++){
#ifndef WEIGHTED
	temp[o+j] = make_pair(v[i].getOutNeighbor(j),i);
#else
	temp[o+j] = make_pair(v[i].getOutNeighbor(j),make_pair(i,v[i].getOutWeight(j)));
#endif
      }
      }}
    free(offsets);
#ifndef WEIGHTED
#ifndef LOWMEM
    intSort::iSort(temp,m,n+1,getFirst<uintE>());
#else
    quickSort(temp,m,pairFirstCmp<uintE>());
#endif
#else
#ifndef LOWMEM
    intSort::iSort(temp,m,n+1,getFirst<intPair>());
#else
    quickSort(temp,m,pairFirstCmp<intPair>());
#endif
#endif
    tOffsets[temp[0].first] = 0;
#ifndef WEIGHTED
    uintE* inEdges = newA(uintE,m);
    inEdges[0] = temp[0].second;
#else
    intE* inEdges = newA(intE,2*m);
    inEdges[0] = temp[0].second.first;
    inEdges[1] = temp[0].second.second;
#endif
    {parallel_for(long i=1;i<m;i++) {
#ifndef WEIGHTED
      inEdges[i] = temp[i].second;
#else
      inEdges[2*i] = temp[i].second.first;
      inEdges[2*i+1] = temp[i].second.second;
#endif
      if(temp[i].first != temp[i-1].first) {
	tOffsets[temp[i].first] = i;
      }
      }}
    free(temp);
    //fill in offsets of degree 0 vertices by taking closest non-zero
    //offset to the right
    sequence::scanIBack(tOffsets,tOffsets,n,minF<uintT>(),(uintT)m);
    {parallel_for(long i=0;i<n;i++){
      uintT o = tOffsets[i];
      uintT l = ((i == n-1) ? m : tOffsets[i+1])-tOffsets[i];
      v[i].setInDegree(l);
#ifndef WEIGHTED
      v[i].setInNeighbors((uintE*)inEdges+o);
#else
      v[i].setInNeighbors((intE*)(inEdges+2*o));
#endif
      }}
    free(tOffsets);
#ifndef WEIGHTED
    Uncompressed_Mem<vertex>* mem = new Uncompressed_Mem<vertex>(v,n,m,edges,inEdges);
    return graph<vertex>(v,n,m,mem);
#else
    Uncompressed_Mem<vertex>* mem = new Uncompressed_Mem<vertex>(v,n,m,edgesAndWeights,inEdges);
    return graph<vertex>(v,n,m,mem);
#endif
  }
  free(offsets);
#ifndef WEIGHTED
  Uncompressed_Mem<vertex>* mem = new Uncompressed_Mem<vertex>(v,n,m,edges);
  return graph<vertex>(v,n,m,mem);
#else
  Uncompressed_Mem<vertex>* mem = new Uncompressed_Mem<vertex>(v,n,m,edgesAndWeights);
  return graph<vertex>(v,n,m,mem);
#endif
}


struct pvertex_t {
    uintE out_deg;
    uint64_t residue;
}; 


template <class vertex>
graph<vertex> readGraphFromBinaryChunkBuff(char* iFile, bool isSymmetric, bool isMmap, bool debug, 
                                          long buffer, long job, bool update) {
  string baseFile = iFile;
  string configFile = baseFile + ".config";
  string vertFile = baseFile + ".vertex";
  string adjSvFile = baseFile + ".adj.sv";
  string adjChunkFiles = baseFile + ".adj.chunk";

  timer t;
  t.start();

  long n, m, level, sv_size;
  long *end_deg, *chunk_sz, *nchunks;
  ifstream in(configFile.c_str(), ifstream::in);
  in >> n >> m >> level >> sv_size;
  end_deg = new long[level];
  chunk_sz = new long[level];
  nchunks = new long[level];
  for(int i = 0; i < level; i++) 
    in >> end_deg[i] >> chunk_sz[i] >> nchunks[i];
  in.close();
  if(debug){
    cout << "ConfigFile: " << configFile << endl; 
    cout << "n = " << n << ", m = " << m << ", level = " << level << ", sv_size = " << sv_size << endl;
    for(int i = 0; i < level; i++) cout << "end_deg[i] = " << end_deg[i] << ", chunk_sz[i] = " << chunk_sz[i] << ", nchunks[i] = " << nchunks[i] << endl;
  }
  
  // calculate DRAM buffer size per layer
  size_t *buff_size_per_level = (size_t*)malloc(sizeof(size_t)*level);
  size_t total_buff_size = 0;
  size_t max_buff_size = buffer ? (size_t)buffer * 1024 * 1024 * 1024: (size_t)126 * 1024 * 1024 * 1024 - sizeof(vertex) * n * 2;

  for(int i = 0; i < level; i++) {
    buff_size_per_level[i] = chunk_sz[i] * nchunks[i];
    total_buff_size += buff_size_per_level[i];
  }
  total_buff_size += sv_size;
  for(int i = 0; i < level; i++) {
    printf("buff_size_per_level[%d] = %lu\n", i, buff_size_per_level[i]);
  }

  cout << "max_buff_size = " << max_buff_size / 1024 / 1024 / 1024 << endl;
  cout << "total_buff_size = " << total_buff_size / 1024 / 1024 / 1024 << endl;

  if (total_buff_size > max_buff_size) {
    for(int i = 0; i < level; i++) {
      buff_size_per_level[i] = (size_t)(ceil((buff_size_per_level[i] * (double)max_buff_size / total_buff_size) / chunk_sz[i]) * chunk_sz[i]);
      // printf("buff_size_per_level[%d] = %lu\n", i, buff_size_per_level[i]);
    }
  }
  for(int i = 0; i < level; i++) {
    printf("buff_size_per_level[%d] = %lu\n", i, buff_size_per_level[i]);
  }

  // size_t BUFF_SIZE = 64 * 1024 * 1024; // size in KB, --> 32GB
  // dram_4kb = 16;
  // dram_2mb = 32;
  // size_t buff_size_per_level[2] = { (size_t)dram_4kb * 1024 * 1024, (size_t)dram_2mb * 1024 * 1024 };
  ChunkBuffer** cbuffs = new ChunkBuffer*[level];
  for(int i = 0; i < level; i++) {
    if(nchunks[i] > 0){
      long nmchunks = buff_size_per_level[i] / chunk_sz[i];
      cout << "BUFF_SIZE = " << buff_size_per_level[i] << ", chunk_sz = " << chunk_sz[i] << ", nmchunks = " << nmchunks << ", nchunks = " << nchunks[i] << endl;
      string adjChunkFile = adjChunkFiles + to_string(i);
      cbuffs[i] = new ChunkBuffer(adjChunkFile.c_str(),chunk_sz[i],nchunks[i],nchunks[i] <= nmchunks? nchunks[i]: nmchunks, job, update);
    } else {
      cbuffs[i] = 0;
    }
    t.reportNext("Load Chunk_" + to_string(i) + " Adjlist Time");
  }
  free(buff_size_per_level);

  // char* edges_chunks_4kb = getFileData(adjChunk4kbFile, nchunks * 4096, isMmap);
  char* edges_sv = 0;
  if(sv_size > 0) edges_sv = getFileData(adjSvFile.c_str(), sv_size, isMmap); // -m for read file by mmap
  t.reportNext("Load Supervertex Adjlist Time");
  pvertex_t* offsets = (pvertex_t*) getFileData(vertFile.c_str(), sizeof(pvertex_t) * n * 2, 0);
  t.reportNext("Load MetaData Time");

  vertex* v = newA(vertex,n);
  // setWorkers(16);
  {parallel_for(long i=0;i<n;i++) {
    uintE d = offsets[i].out_deg;
    uint64_t r = offsets[i].residue;
      // cout << i << " " << d << " " << r << endl; // correct
      v[i].setOutDegree(d);
      if(d==0){
        v[i].setOutNeighbors(0);
      }else if(d<=end_deg[level-1]){
        v[i].setOutNeighbors((uintE*)r);
      }else{
        uintE* nebrs = (uintE*)(edges_sv+r);//+8); // 8B for pblk header (max_count/count) in HG, removed
        v[i].setOutNeighbors(nebrs);
      }
    }}
  t.reportNext("Load OutNeighbors Time");
  if(!isSymmetric) {
    {parallel_for(long i=0;i<n;i++) {
    uintT d = offsets[n+i].out_deg;
    uintT r = offsets[n+i].residue;
      v[i].setInDegree(d);
      if(d <= end_deg[level-1]){
        v[i].setInNeighbors((uintE*)r);
      }else{
        uintE* nebrs = (uintE*)(edges_sv+r);//+8); // 8B for pblk header (max_count/count) in HG, removed
        v[i].setInNeighbors(nebrs);
      }}}}
  free(offsets);
  t.reportNext("Load InNeighbors Time");

  Uncompressed_Mem<vertex>* mem = new Uncompressed_Mem<vertex>(v,n,m,0,edges_sv);
  t.stop();
  t.reportTotal("Read Graph Time");
  cout << "readGraphFromBinaryChunkBuff end.\n" << endl;
  return graph<vertex>(v,n,m,mem,level,end_deg,cbuffs);
}

template <class vertex>
graph<vertex> readGraphFromChunk(char* iFile, bool isSymmetric, bool isMmap, bool debug, 
                                          long buffer, long job=0, bool update=0) {
  timer t;
  pid_t pid = getpid();
  vm_reporter reporter(pid);

  TriLevelManager* manager = new TriLevelManager();
  TriLevelReader* reader = manager->getReader();

  t.start();
  reporter.reportNext("Start Read Graph Space");
  reader->readConfig(iFile, debug);
  long m = reader->m, n = reader->n, level = reader->level;
  t.reportNext("Load Config Time");
  reporter.reportNext("Load Config Space");
  
  manager->init();
  t.reportNext("Load ChunkManager Time");
  reporter.reportNext("Load ChunkManager Space");

  std::string vertFile = reader->vertFile;
  pvertex_t* offsets = (pvertex_t*)getFileData(vertFile.c_str(), sizeof(pvertex_t) * n * 2, 0);
  t.reportNext("Load MetaData Time");
  reporter.reportNext("Load MetaData Space");

  vertex* v = newA(vertex,n);
  t.reportNext("Allocate Vertex Time");
  reporter.reportNext("Allocate Vertex Space");

  char* edges_sv = manager->getSVAddr(0);
  char* redges_sv = manager->getSVAddr(1);

  printf("edges_sv = %p, redges_sv = %p\n", edges_sv, redges_sv);

  {parallel_for(long i=0;i<n;i++) {
    uintE d = offsets[i].out_deg;
    uint64_t r = offsets[i].residue;
    // cout << i << " " << d << " " << r << endl; // correct
    v[i].setOutDegree(d);
    if (d == 0) {
      v[i].setOutNeighbors(0);
    } else if (d <= reader->end_deg[level-1]) {
      v[i].setOutNeighbors((uintE*)r);
    } else {
      uintE* nebrs = (uintE*)(edges_sv+r);
      v[i].setOutNeighbors(nebrs);
    }
  }}
  t.reportNext("Load OutNeighbors Time");
  reporter.reportNext("Load OutNeighbors Space");

  if(!isSymmetric) {
    {parallel_for(long i=0;i<n;i++) {
      uintE d = offsets[n+i].out_deg;
      uint64_t r = offsets[n+i].residue;
      v[i].setInDegree(d);
      if (d==0) {
        v[i].setInNeighbors(0);
      } else if (d <= reader->rend_deg[level-1]) {
        v[i].setInNeighbors((uintE*)r);
      } else {
        uintE* nebrs = (uintE*)(redges_sv+r);
        v[i].setInNeighbors(nebrs);
      }
    }}
    t.reportNext("Load InNeighbors Time");
    reporter.reportNext("Load InNeighbors Space");
  }
  free(offsets);

  Uncompressed_ChunkMem<vertex>* mem = new Uncompressed_ChunkMem<vertex>(v,n,m);
  t.stop();
  t.reportTotal("Read Graph Time");
  cout << "readGraphFromChunk end.\n" << endl;

  return graph<vertex>(v,n,m,mem,manager);
}

template <class vertex>
graph<vertex> readGraph(char* iFile, bool compressed, bool symmetric, bool binary, bool mmap, 
                        long job=0, bool update=0, bool chunk=0, bool debug=0, long buffer=0) {
  if(binary){ 
    // if(chunk) return readGraphFromBinaryChunkBuff<vertex>(iFile,symmetric,mmap,debug,buffer,job,update);
    if (chunk) return readGraphFromChunk<vertex>(iFile, symmetric, mmap, debug, buffer);
    return readGraphFromBinary<vertex>(iFile,symmetric);
  }
  else return readGraphFromFile<vertex>(iFile,symmetric,mmap);
}

template <class vertex>
graph<vertex> readCompressedGraph(char* fname, bool isSymmetric, bool mmap) {
  char* s;
  if (mmap) {
    _seq<char> S = mmapStringFromFile(fname);
    // Cannot mutate graph unless we copy.
    char *bytes = newA(char, S.n);
    parallel_for(size_t i=0; i<S.n; i++) {
      bytes[i] = S.A[i];
    }
    if (munmap(S.A, S.n) == -1) {
      perror("munmap");
      exit(-1);
    }
    s = bytes;
  } else {
    ifstream in(fname,ifstream::in |ios::binary);
    in.seekg(0,ios::end);
    long size = in.tellg();
    in.seekg(0);
    cout << "size = " << size << endl;
    s = (char*) malloc(size);
    in.read(s,size);
    in.close();
  }

  long* sizes = (long*) s;
  long n = sizes[0], m = sizes[1], totalSpace = sizes[2];

  cout << "n = "<<n<<" m = "<<m<<" totalSpace = "<<totalSpace<<endl;
  cout << "reading file..."<<endl;

  uintT* offsets = (uintT*) (s+3*sizeof(long));
  long skip = 3*sizeof(long) + (n+1)*sizeof(intT);
  uintE* Degrees = (uintE*) (s+skip);
  skip+= n*sizeof(intE);
  uchar* edges = (uchar*)(s+skip);

  uintT* inOffsets;
  uchar* inEdges;
  uintE* inDegrees;
  if(!isSymmetric){
    skip += totalSpace;
    uchar* inData = (uchar*)(s + skip);
    sizes = (long*) inData;
    long inTotalSpace = sizes[0];
    cout << "inTotalSpace = "<<inTotalSpace<<endl;
    skip += sizeof(long);
    inOffsets = (uintT*) (s + skip);
    skip += (n+1)*sizeof(uintT);
    inDegrees = (uintE*)(s+skip);
    skip += n*sizeof(uintE);
    inEdges = (uchar*)(s + skip);
  } else {
    inOffsets = offsets;
    inEdges = edges;
    inDegrees = Degrees;
  }


  vertex *V = newA(vertex,n);
  parallel_for(long i=0;i<n;i++) {
    long o = offsets[i];
    uintT d = Degrees[i];
    V[i].setOutDegree(d);
    V[i].setOutNeighbors(edges+o);
  }

  if(sizeof(vertex) == sizeof(compressedAsymmetricVertex)){
    parallel_for(long i=0;i<n;i++) {
      long o = inOffsets[i];
      uintT d = inDegrees[i];
      V[i].setInDegree(d);
      V[i].setInNeighbors(inEdges+o);
    }
  }

  cout << "creating graph..."<<endl;
  Compressed_Mem<vertex>* mem = new Compressed_Mem<vertex>(V, s);

  graph<vertex> G(V,n,m,mem);
  return G;
}
