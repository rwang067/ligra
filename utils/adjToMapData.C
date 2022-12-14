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
#include "parseCommandLine.h"
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
void genMapData(char* fname, bool isSymmetric) {
  _seq<char> S = readStringFromFile(fname);
  words W = stringToWords(S.A, S.n);
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

  const char suffix[] = "_dump"; 
  strcat(fname, suffix);
  int fd = open(fname, O_RDWR | O_CREAT, 0666);
  if (fd == -1) {
    perror("open");
    exit(-1);
  }
#ifndef WEIGHTED
  long size = 1 + sizeof(long) * 2 + sizeof(vertex) * n + sizeof(uintE) * m;
  if(!isSymmetric) size += sizeof(uintE) * m;
#else
  long size = 1 + sizeof(long) * 2 + sizeof(vertex) * n + sizeof(intE) * 2 * m;
  if(!isSymmetric) size += sizeof(intE) * 2 * m;
#endif
  if(ftruncate(fd, size) == -1)
  {
    perror("ftruncate");
    exit(-1);
  }
  char *p = static_cast<char*>(mmap(0, size, PROT_WRITE, MAP_SHARED, fd, 0));
  if ((long)p == -1) {
    perror("mmap");
    exit(-1);
  }
  *p = isSymmetric;
  *(long*)(p + 1) = n;
  *(long*)(p + 1 + sizeof(long)) = m;
  long fileOffset = 1 + sizeof(long) * 2;

  uintT* offsets = newA(uintT,n);
#ifndef WEIGHTED
  uintE* edges = (uintE*)(p + fileOffset);
  fileOffset += sizeof(uintE) * m;
#else
  intE* edges = (intE*)(p + fileOffset);
  fileOffset += sizeof(intE) * 2 * m;
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

  vertex* v = (vertex*)(p + fileOffset);
  fileOffset += sizeof(vertex) * n;

  if(!isSymmetric) {
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
    uintE* inEdges = (uintE*)(p + fileOffset);
    inEdges[0] = temp[0].second;
#else
    intE* inEdges = (intE*)(p + fileOffset);
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
      v[i].setInNeighbors((uintE*)o);
      v[i].setOutNeighbors((uintE*)(v[i].getOutNeighbors() - edges));
#else
      v[i].setInNeighbors((intE*)(2*o));
      v[i].setOutNeighbors((intE*)(v[i].getOutNeighbors() - edges));
#endif
      }}

    free(tOffsets);
  }
  else {
    {parallel_for (uintT i=0; i < n; i++) {
        uintT o = offsets[i];
        uintT l = ((i == n-1) ? m : offsets[i+1])-offsets[i];
        v[i].setOutDegree(l);
#ifndef WEIGHTED
        v[i].setOutNeighbors((uintE*)o);
#else
        v[i].setOutNeighbors((intE*)(2*o));
#endif
    }}
    free(offsets);
  }
  close(fd);
  munmap(p, size);
}

int parallel_main(int argc, char* argv[]) {
  commandLine P(argc,argv,"[-s] <inFile>");
  char* iFile = P.getArgument(0);
  bool symmetric = P.getOptionValue("-s");
  if(symmetric) genMapData<symmetricVertex>(iFile, symmetric);
  else genMapData<asymmetricVertex>(iFile, symmetric);
  return 0;
}