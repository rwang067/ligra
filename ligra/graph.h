#ifndef GRAPH_H
#define GRAPH_H
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include "vertex.h"
#include "compressedVertex.h"
#include "parallel.h"
#include "chunk_buffer.h"
using namespace std;

  // template <class vertex>
  // inline uintE* getChunkNeighbors(graph<vertex>& GA, vertex* v, bool inGraph) {
  //   uintE d;
  //   uintE* neighbors;
  //   if(!inGraph){
  //     d = v->getOutDegree();
  //     neighbors = v->getOutNeighbors();
  //   }else{
  //     d = v->getInDegree();
  //     neighbors = v->getInNeighbors();
  //   }
  // #ifdef CHUNK
  //   if(d > 2 && d<=254){ 
  //     uint64_t r = (uint64_t)neighbors;
  //     uint32_t cid = r >> 32;
  //     uint32_t coff = r & 0xFFFFFFFF;
  //     char* mchunk = GA.D->cbuff->get_mchunk(cid);
  //     return (uintE*)(mchunk+coff);
  //   };
  // #endif
  //   return neighbors;
  // }

// **************************************************************
//    ADJACENCY ARRAY REPRESENTATION
// **************************************************************

// Class that handles implementation specific freeing of memory
// owned by the graph
struct Deletable {
public:
  virtual void del() = 0;
};

template <class vertex>
struct Uncompressed_Mem : public Deletable {
public:
  vertex* V;
  long n;
  long m;
  void* allocatedInplace, * inEdges;

  Uncompressed_Mem(vertex* VV, long nn, long mm, void* ai, void* _inEdges = NULL)
  : V(VV), n(nn), m(mm), allocatedInplace(ai), inEdges(_inEdges){ }

  void del() {
    if (allocatedInplace == NULL)
      for (long i=0; i < n; i++) V[i].del();
    // else free(allocatedInplace);
    free(V);
    // if(inEdges != NULL) free(inEdges);
  }
};

template <class vertex>
struct Uncompressed_Memhypergraph : public Deletable {
public:
  vertex* V;
  vertex* H;
  long nv;
  long mv;
  long nh;
  long mh;
  void* edgesV, *inEdgesV, *edgesH, *inEdgesH;

 Uncompressed_Memhypergraph(vertex* VV, vertex* HH, long nnv, long mmv, long nnh, long mmh, void* _edgesV, void* _edgesH, void* _inEdgesV = NULL, void* _inEdgesH = NULL)
   : V(VV), H(HH), nv(nnv), mv(mmv), nh(nnh), mh(mmh), edgesV(_edgesV), edgesH(_edgesH), inEdgesV(_inEdgesV), inEdgesH(_inEdgesH) { }

  void del() {
    free(edgesV);
    free(edgesH);
    free(V);
    free(H);
    if(inEdgesV != NULL) free(inEdgesV);
    if(inEdgesH != NULL) free(inEdgesH);
  }
};

template <class vertex>
struct Compressed_Mem : public Deletable {
public:
  vertex* V;
  char* s;

  Compressed_Mem(vertex* _V, char* _s) :
                 V(_V), s(_s) { }

  void del() {
    free(V);
    free(s);
  }
};

template <class vertex>
struct Compressed_Memhypergraph : public Deletable {
public:
  vertex* V;
  vertex* H;
  char* s;

 Compressed_Memhypergraph(vertex* _V, vertex* _H, char* _s) :
  V(_V), H(_H), s(_s) { }

  void del() {
    free(V);
    free(H);
    free(s);
  }
};

template <class vertex>
struct graph {
  vertex *V;
  long n;
  long m;
  bool transposed;
  uintE* flags;
  Deletable *D;
  ChunkBuffer* cbuff;

graph(vertex* _V, long _n, long _m, Deletable* _D) : V(_V), n(_n), m(_m),
  D(_D), flags(NULL), transposed(0), cbuff(NULL) {}

graph(vertex* _V, long _n, long _m, Deletable* _D, uintE* _flags) : V(_V),
  n(_n), m(_m), D(_D), flags(_flags), transposed(0), cbuff(NULL) {}

graph(vertex* _V, long _n, long _m, Deletable* _D, ChunkBuffer* _cbuff) : V(_V),
  n(_n), m(_m), D(_D), cbuff(_cbuff), transposed(0) {}

  void del() {
    // if (flags != NULL) free(flags);
    // D->del();
    // free(D);
    cbuff->del();
    free(cbuff);
  }

  void transpose() {
    if ((sizeof(vertex) == sizeof(asymmetricVertex)) ||
        (sizeof(vertex) == sizeof(compressedAsymmetricVertex))) {
      parallel_for(long i=0;i<n;i++) {
        V[i].flipEdges();
      }
      transposed = !transposed;
    }
  }

  inline uintE* getChunkNeighbors(vertex* v, bool inGraph) {
    uintE d;
    uintE* neighbors;
    if(!inGraph){
      d = v->getOutDegree();
      neighbors = (uintE*) v->getOutNeighbors();
    }else{
      d = v->getInDegree();
      neighbors = (uintE*) v->getInNeighbors();
    }
  #ifdef CHUNK
    if(d > 2 && d<=254){ 
      uint64_t r = (uint64_t)neighbors;
      uint32_t cid = r >> 32;
      uint32_t coff = r & 0xFFFFFFFF;
      cout << "r = " << r << ", cid = " << cid << ", coff = " << coff << endl;
      char* mchunk = cbuff->get_mchunk(cid);
      return (uintE*)(mchunk+coff+8);// 8B for pblk header in HG
    };
  #endif
    return neighbors;
  }
};

template <class vertex>
struct hypergraph {
  vertex *V;
  vertex *H;
  long nv;
  long mv;
  long nh;
  long mh;
  bool transposed;
  uintE* flags;
  Deletable *D;

hypergraph(vertex* _V, vertex* _H, long _nv, long _mv, long _nh, long _mh, Deletable* _D) : V(_V), H(_H), nv(_nv), mv(_mv), nh(_nh), mh(_mh),
    D(_D), flags(NULL), transposed(0) {}

hypergraph(vertex* _V, vertex* _H, long _nv, long _mv, long _nh, long _mh,  Deletable* _D, uintE* _flags) : V(_V), H(_H),
    nv(_nv), mv(_mv), nh(_nh), mh(_mh), D(_D), flags(_flags), transposed(0) {}

  void del() {
    if (flags != NULL) free(flags);
    D->del();
    free(D);
  }
  void initFlags() {
    flags = newA(uintE,max(nv,nh));
    parallel_for(long i=0;i<max(nv,nh);i++) flags[i]=UINT_E_MAX;
  }
  
  void transpose() {
    if ((sizeof(vertex) == sizeof(asymmetricVertex)) ||
        (sizeof(vertex) == sizeof(compressedAsymmetricVertex))) {
      parallel_for(long i=0;i<nv;i++) {
        V[i].flipEdges();
      }
      parallel_for(long i=0;i<nh;i++) {
	H[i].flipEdges();
      }
      transposed = !transposed;
    }
  }
};
#endif
