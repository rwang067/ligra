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
#include <cassert>
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

#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
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

struct pvertex_t {
    uintE out_deg;
    uint64_t residue;
}; 
char* getFileData(const char* filename, size_t size = 0, bool isMmap = 0){
  char* addr = 0;
  if(!isMmap){
    ifstream in(filename,ifstream::in | ios::binary); //stored as uints
    in.seekg(0, ios::end);
    long size1 = in.tellg();
    in.seekg(0);
    if(size1 != size){ 
      cout << size1 << " " << size << std::endl;
      cout << "Filename size wrong for :" << filename << std::endl; 
      cout << "Specified size = " << size << ", read size = " << size1 << std::endl;
      abort(); 
    }
    addr = (char *) malloc(size);
    in.read(addr,size);
    in.close();
  } else {
    int fd = open(filename, O_RDWR|O_CREAT, 00777);
    if(fd == -1){
      std::cout << "Could not open file for :" << filename << " error: " << strerror(errno) << std::endl;
      exit(1);
    }
    if(ftruncate(fd, size) == -1){
      std::cout << "Could not ftruncate file for :" << filename << " error: " << strerror(errno) << std::endl;
      close(fd);
      exit(1);
    }
    // addr = (char*)mmap(NULL, size, PROT_READ|PROT_WRITE,MAP_SHARED, fd, 0);
    addr = (char*)mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
    // // mmap with 2MB huge pages
    // addr = (char*)mmap(NULL, size, PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB|MAP_HUGE_2MB, 0, 0);
    if(addr == MAP_FAILED) {	
      std::cout << "Could not mmap file for :" << filename << " error: " << strerror(errno) << std::endl;
      close(fd);
      exit(1);
    } else {
      std::cout << "mmap succeeded, size = " << size << ",filename = " << filename << std::endl;
    }
    // preada(fd, addr, size, 0);
  }
  return addr;
}

char* anonymousMmap(size_t size) {
  char* addr = (char*)mmap(NULL, size, PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS, 0, 0);
  if(addr == MAP_FAILED) {	
    std::cout << "Could not mmap file for anonymous mmap, error: " << strerror(errno) << std::endl;
    exit(1);
  } else {
    std::cout << "mmap succeeded, size = " << size << std::endl;
  }
  return addr;
}

uint32_t getLevel(uintE d, long level, long* end_deg) {
  for (uint32_t i = 0; i < level; i++) {
    if (d <= end_deg[i]) {
      return i;
    }
  }
  perror("Illegal degree");
  exit(1);
}

template <class vertex>
graph<vertex> readGraphFromChunk(char* iFile, bool isSymmetric) {
  string baseFile = iFile;
  string configFile = baseFile + ".config";
  string vertFile = baseFile + ".vertex";
  string adjSvFile = baseFile + ".adj.sv";
  string adjChunkFiles = baseFile + ".adj.chunk";

  timer t;
  t.start();

  bool debug = 1;
  bool isMmap = 0;

  long n, m, level, sv_size;
  long *end_deg, *chunk_sz, *nchunks, *level_sz;

  ifstream in(configFile.c_str(), ifstream::in);
  in >> n >> m >> level >> sv_size;
  end_deg = new long[level];
  chunk_sz = new long[level];
  nchunks = new long[level];
  level_sz = new long[level];

  for(int i = 0; i < level; i++) {
    in >> end_deg[i] >> chunk_sz[i] >> nchunks[i];
    level_sz[i] = nchunks[i] * chunk_sz[i];
  }
  in.close();

  if(debug){
    cout << "ConfigFile: " << configFile << endl; 
    cout << "n = " << n << ", m = " << m << ", level = " << level << ", sv_size = " << sv_size << endl;
    for(int i = 0; i < level; i++) {
      cout << "end_deg[" << i << "] = " << end_deg[i] << ", chunk_sz[" << i << "] = " << chunk_sz[i] << ", nchunks[" << i << "] = " << nchunks[i] << ", level_sz[" << i << "] = " << level_sz[i] << endl;
    }
  }

  char** cbuffs = new char*[level];
  for(int i = 0; i < level; i++) {
    string adjChunkFile = adjChunkFiles + to_string(i);
    cbuffs[i] = getFileData(adjChunkFile.c_str(), level_sz[i], 0); // -m for read file by mmap
    t.reportNext("Load Chunk_" + to_string(i) + " Adjlist Time");
  }

  char* edges_sv = 0;
  if(sv_size > 0) edges_sv = getFileData(adjSvFile.c_str(), sv_size, 0); // -m for read file by mmap
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
      } else if(d<=2){
        v[i].setOutNeighbors((uintE*)(r));
      } else if(d<=end_deg[level-1]){
        uint32_t cid = r >> 32;
        uint32_t coff = r & 0xFFFFFFFF;
        uint32_t lv = getLevel(d, level, end_deg);
        uint64_t offset = cid * chunk_sz[lv] + coff;
        uintE* nebrs = (uintE*)(cbuffs[lv]+offset);
        v[i].setOutNeighbors(nebrs);
      } else{
        uintE* nebrs = (uintE*)(edges_sv+r);//+8); // 8B for pblk header (max_count/count) in HG, removed
        v[i].setOutNeighbors(nebrs);
      }
    }}
  t.reportNext("Load OutNeighbors Time");
  if(!isSymmetric) {
    {parallel_for(long i=0;i<n;i++) {
    uintE d = offsets[n+i].out_deg;
    uint64_t r = offsets[n+i].residue;
      // cout << i << " " << d << " " << r << endl; // correct
      v[i].setInDegree(d);
      if(d==0){
        v[i].setInNeighbors(0);
      } else if(d<=2){
        v[i].setInNeighbors((uintE*)(r));
      } else if(d<=end_deg[level-1]){
        uint32_t cid = r >> 32;
        uint32_t coff = r & 0xFFFFFFFF;
        uint32_t lv = getLevel(d, level, end_deg);
        uint64_t offset = cid * chunk_sz[lv] + coff;
        uintE* nebrs = (uintE*)(cbuffs[lv]+offset);
        v[i].setInNeighbors(nebrs);
      } else{
        uintE* nebrs = (uintE*)(edges_sv+r);//+8); // 8B for pblk header (max_count/count) in HG, removed
        v[i].setInNeighbors(nebrs);
      }
    }}}
  free(offsets);
  t.reportNext("Load InNeighbors Time");

  Uncompressed_Mem<vertex>* mem = new Uncompressed_Mem<vertex>(v,n,m,0,edges_sv);
  t.stop();
  t.reportTotal("Read Graph Time");
  cout << "readGraphFromBinaryChunkBuff end.\n" << endl;

  delete[] end_deg;
  delete[] chunk_sz;
  delete[] nchunks;
  delete[] level_sz;
  
  return graph<vertex>(v,n,m,mem);
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
    char *suffix = (char*) ".radj"; 
    char radjFile[strlen(iFile)+strlen(suffix)+1];
    *radjFile = 0;
    strcat(radjFile, iFile);
    strcat(radjFile, suffix);
    suffix = (char*) ".ridx"; 
    char ridxFile[strlen(iFile)+strlen(suffix)+1];
    *ridxFile = 0;
    strcat(ridxFile, iFile);
    strcat(ridxFile, suffix);

#ifndef WEIGHTED
    uintE* inEdges;
#else
    intE* inEdges = newA(intE,2*m);
#endif
    ifstream in4(radjFile,ifstream::in | ios::binary);
    in4.seekg(0, ios::end);
    size = in4.tellg();
    in4.seekg(0);
    inEdges = (uintE*) malloc(size);
    in4.read((char*)inEdges, size);
    in4.close();

    ifstream in5(ridxFile,ifstream::in | ios::binary); //stored as longs
    in5.seekg(0, ios::end);
    size = in5.tellg();
    in5.seekg(0);
    if(n != size/sizeof(intT)) { 
      cout << n << " " << size << " " << sizeof(intT) << " " << size/sizeof(intT) << " " << size/8 << std::endl;
      cout << "ridx: File size wrong\n"; abort(); 
    }
    in5.read((char*)offsets,size);
    in5.close();

    {parallel_for(long i=0;i<n;i++){
      uintT o = offsets[i];
      uintT l = ((i == n-1) ? m : offsets[i+1])-offsets[i];
      v[i].setInDegree(l);
#ifndef WEIGHTED
      v[i].setInNeighbors((uintE*)inEdges+o);
#else
      v[i].setInNeighbors((intE*)(inEdges+2*o));
#endif
    }}
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

template <class vertex>
graph<vertex> readGraphFromBinarymmap(char* iFile, bool isSymmetric) {
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

  pid_t pid = getpid();
  vm_reporter reporter(pid);
  reporter.print("Start reading graph from binary file");

  timer time;
  time.start();

  ifstream in(configFile, ifstream::in);
  long n;
  in >> n;
  in.close();

  time.reportNext("Load Config Time");
  reporter.reportNext("Load Config Space");

  ifstream in2(adjFile,ifstream::in | ios::binary); //stored as uints
  in2.seekg(0, ios::end);
  long size = in2.tellg();
  in2.seekg(0);
#ifdef WEIGHTED
  long m = size/(2*sizeof(uint));
#else
  long m = size/sizeof(uint);
#endif
  in2.close();
  time.reportNext("This shouldn't take long");
  uintE* edges = (uintE*) getFileData(adjFile, size, 1); // -m for read file by mmap
  time.reportNext("Load Adjlist Time");
  reporter.reportNext("Load Adjlist Space");

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
  time.reportNext("Load Index Time");
  reporter.reportNext("Load Index Space");

  vertex* v = newA(vertex,n);
  time.reportNext("Allocate vertex time");
  reporter.reportNext("Allocate vertex space");
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
  time.reportNext("Load OutNeighbors Time");
  reporter.reportNext("Load OutNeighbors Space");

  if(!isSymmetric) {
    char *suffix = (char*) ".radj"; 
    char radjFile[strlen(iFile)+strlen(suffix)+1];
    *radjFile = 0;
    strcat(radjFile, iFile);
    strcat(radjFile, suffix);
    suffix = (char*) ".ridx"; 
    char ridxFile[strlen(iFile)+strlen(suffix)+1];
    *ridxFile = 0;
    strcat(ridxFile, iFile);
    strcat(ridxFile, suffix);

#ifndef WEIGHTED
    uintE* inEdges;
#else
    intE* inEdges = newA(intE,2*m);
#endif
    ifstream in4(radjFile,ifstream::in | ios::binary);
    in4.seekg(0, ios::end);
    size = in4.tellg();
    in4.seekg(0);
    in4.close();
    time.reportNext("This shouldn't take long");

    inEdges = (uintE*) getFileData(radjFile, size, 1); // -m for read file by mmap
    time.reportNext("Load InEdges Time");
    reporter.reportNext("Load InEdges Space");

    ifstream in5(ridxFile,ifstream::in | ios::binary); //stored as longs
    in5.seekg(0, ios::end);
    size = in5.tellg();
    in5.seekg(0);
    if(n != size/sizeof(intT)) { 
      cout << n << " " << size << " " << sizeof(intT) << " " << size/sizeof(intT) << " " << size/8 << std::endl;
      cout << "ridx: File size wrong\n"; abort(); 
    }
    in5.read((char*)offsets,size);
    in5.close();

    time.reportNext("Load InIndex Time");
    reporter.reportNext("Load InIndex Space");

    {parallel_for(long i=0;i<n;i++){
      uintT o = offsets[i];
      uintT l = ((i == n-1) ? m : offsets[i+1])-offsets[i];
      v[i].setInDegree(l);
#ifndef WEIGHTED
      v[i].setInNeighbors((uintE*)inEdges+o);
#else
      v[i].setInNeighbors((intE*)(inEdges+2*o));
#endif
    }}
    time.reportNext("Load InNeighbors Time");
    reporter.reportNext("Load InNeighbors Space");
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

template <class vertex>
graph<vertex> readGraph(char* iFile, bool compressed, bool symmetric, bool binary, bool mmap, bool chunk=0) {
  if (binary) {
    if (chunk) return readGraphFromChunk<vertex>(iFile,symmetric);
    // return readGraphFromBinary<vertex>(iFile,symmetric);
    return readGraphFromBinarymmap<vertex>(iFile,symmetric);
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
