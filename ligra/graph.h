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

// #define CHUNK_MMAP

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

// template <class vertex>
// struct graph {
//   vertex *V;
//   long n;
//   long m;
//   bool transposed;
//   uintE* flags;
//   Deletable *D;
//   long chunk_level;
//   long *end_deg;
//   ChunkBuffer** cbuffs;

// graph(vertex* _V, long _n, long _m, Deletable* _D) : V(_V), n(_n), m(_m),
//   D(_D), flags(NULL), transposed(0), chunk_level(0), end_deg(NULL), cbuffs(NULL) {}

// graph(vertex* _V, long _n, long _m, Deletable* _D, uintE* _flags) : V(_V),
//   n(_n), m(_m), D(_D), flags(_flags), transposed(0), chunk_level(0), end_deg(NULL), cbuffs(NULL) {}

// graph(vertex* _V, long _n, long _m, Deletable* _D, long _level, long* _end_deg, ChunkBuffer** _cbuffs) : V(_V),
//   n(_n), m(_m), D(_D), chunk_level(_level), end_deg(_end_deg), cbuffs(_cbuffs), transposed(0) {}

//   void del() {
//     // if (flags != NULL) free(flags);
//     // D->del();
//     // free(D);
//     for(int i = 0; i < chunk_level; i++)
//       cbuffs[i]->del();
//     delete [] cbuffs;
//     delete [] end_deg;
//   }

//   void transpose() {
//     if ((sizeof(vertex) == sizeof(asymmetricVertex)) ||
//         (sizeof(vertex) == sizeof(compressedAsymmetricVertex))) {
//       parallel_for(long i=0;i<n;i++) {
//         V[i].flipEdges();
//       }
//       transposed = !transposed;
//     }
//   }

//   inline uintE* getChunkNeighbors(vertex* v, bool inGraph) {
//     uintE d;
//     uintE* neighbors;
//     if(!inGraph){
//       d = v->getOutDegree();
//       neighbors = (uintE*) v->getOutNeighbors(); // if d <=2 return (uintE*)(&outNeighbors); in vertex.h
//     }else{
//       d = v->getInDegree();
//       neighbors = (uintE*) v->getInNeighbors();
//     }
//   #ifdef CHUNK
//     if(d > 2 && d <= end_deg[chunk_level-1]) {
//       uint64_t r = (uint64_t)neighbors;
//       uint32_t cid = r >> 32;
//       uint32_t coff = r & 0xFFFFFFFF;
//       for (int i = 0; i < chunk_level; ++i) {
//         if (d <= end_deg[i]) return cbuffs[i]->get_nebrs_from_mchunk(cid, coff, d);
//       }
//     }
//     // if(d > 2 && d <= end_deg[chunk_level-1]){ 
//     //   uint64_t r = (uint64_t)neighbors;
//     //   uint32_t cid = r >> 32;
//     //   uint32_t coff = r & 0xFFFFFFFF;
//     //   // cout << "r = " << r << ", cid = " << cid << ", coff = " << coff << endl;

//     //   if(chunk_level > 0 && d <= end_deg[0])
//     //     return cbuffs[0]->get_nebrs_from_mchunk(cid, coff, d);
//     //   if(chunk_level > 1 && d <= end_deg[1])
//     //     return cbuffs[1]->get_nebrs_from_mchunk(cid, coff, d);
//     //   if(chunk_level > 2 && d <= end_deg[2])
//     //     return cbuffs[2]->get_nebrs_from_mchunk(cid, coff, d);
//     // }

//     // if(d > 2 && d<=1022){ 
//     //   uint64_t r = (uint64_t)neighbors;
//     //   uint32_t cid = r >> 32;
//     //   uint32_t coff = r & 0xFFFFFFFF;
//     //   // cout << "r = " << r << ", cid = " << cid << ", coff = " << coff << endl;
//     //   return cbuff->get_nebrs_from_mchunk(cid, coff, d);

//     //   // uintE* nebrs = cbuff->get_nebrs_from_mchunk(cid, coff, d);
//     //   // vertex* v1; uintE d1; uintE* neighbors1;
//     //   // if (d < 1000) {
//     //   //   for(uintE i = 0; i < d; i++){
//     //   //     v1 = &V[nebrs[i]];
//     //   //     if(!inGraph) d1 = v1->getOutDegree();
//     //   //     else d1 = v1->getInDegree();
//     //   //     if(d1 > 2 && d1 <= 1022){ 
//     //   //       if(!inGraph) neighbors1 = (uintE*) v1->getOutNeighbors();
//     //   //       else neighbors1 = (uintE*) v1->getInNeighbors();
//     //   //       uint32_t cid1 = ((uint64_t)neighbors1) >> 32;
//     //   //       cbuff->update_chunk_hot(cid, d1*sizeof(uintE));
//     //   //     }
//     //   //   }
//     //   // } else {
//     //   //   parallel_for(uintE i = 0; i < d; i++){
//     //   //     v1 = &V[nebrs[i]];
//     //   //     if(!inGraph) d1 = v1->getOutDegree();
//     //   //     else d1 = v1->getInDegree();
//     //   //     if(d1 > 2 && d1 <= 1022){ 
//     //   //       if(!inGraph) neighbors1 = (uintE*) v1->getOutNeighbors();
//     //   //       else neighbors1 = (uintE*) v1->getInNeighbors();
//     //   //       uint32_t cid1 = ((uint64_t)neighbors1) >> 32;
//     //   //       cbuff->update_chunk_hot(cid, d1*sizeof(uintE));
//     //   //     }
//     //   //   }
//     //   // }
//     //   // return nebrs;
//     // };
//   #endif
//     return neighbors;
//   }
// };

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

template <class vertex>
struct Uncompressed_ChunkMem : public Deletable {
public:
  vertex* V;
  long n;
  long m;

  Uncompressed_ChunkMem(vertex* VV, long nn, long mm) : V(VV), n(nn), m(mm) {}

  void del() {
    for (long i=0; i < n; i++) V[i].del();
    free(V);
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
  TriLevelManager* manager;

  graph() : V(NULL), n(0), m(0), D(NULL), flags(NULL), transposed(0), manager(NULL) {}

  graph(vertex* _V, long _n, long _m, Deletable* _D) : V(_V), n(_n), m(_m),
    D(_D), flags(NULL), transposed(0), manager(NULL) {}

  graph(vertex* _V, long _n, long _m, Deletable* _D, uintE* _flags) : V(_V), n(_n), m(_m), 
    D(_D), flags(_flags), transposed(0), manager(NULL) {}

  graph(vertex* _V, long _n, long _m, Deletable* _D, TriLevelManager* _manager) : V(_V), n(_n), m(_m),
    D(_D), flags(NULL), transposed(0), manager(_manager) {}

  void del() {
    if (flags != NULL) free(flags);
    D->del();
    free(D);
  }

  void transpose() {
    if ((sizeof(vertex) == sizeof(asymmetricVertex)) ||
        (sizeof(vertex) == sizeof(compressedAsymmetricVertex))) {
      parallel_for(long i=0;i<n;i++) {
        V[i].flipEdges();
      }
      transposed = !transposed;

      if (manager != NULL) {
        manager->transpose();
      }
    }
  }

  inline uintE* getChunkNeighbors(vertex* v, bool inGraph) {
    uintE d;
    uintE* neighbors;
    long threshold;
    if(!inGraph) {
      d = v->getOutDegree();
      neighbors = (uintE*) v->getOutNeighbors(); // if d <=2 return (uintE*)(&outNeighbors); in vertex.h
      
    } else {
      d = v->getInDegree();
      neighbors = (uintE*) v->getInNeighbors();
    }
    #ifdef CHUNK
    #ifndef CHUNK_MMAP
    if (!inGraph) {
      long* end_deg = manager->getReader()->end_deg;
      long level = manager->getReader()->level;
      if(d > 2 && d <= end_deg[level-1]) {
        uint64_t r = (uint64_t)neighbors;
        uint32_t cid = r >> 32;
        uint32_t coff = r & 0xFFFFFFFF;
        for (int i = 0; i < level; ++i) {
          if (d <= end_deg[i]) return manager->getChunkNeighbors(cid, coff, i, d, inGraph);
        }
      }
    } else {
      long* end_deg = manager->getReader()->rend_deg;
      long level = manager->getReader()->level;
      if(d > 2 && d <= end_deg[level-1]) {
        uint64_t r = (uint64_t)neighbors;
        uint32_t cid = r >> 32;
        uint32_t coff = r & 0xFFFFFFFF;
        for (int i = 0; i < level; ++i) {
          if (d <= end_deg[i]) return manager->getChunkNeighbors(cid, coff, i, d, inGraph);
        }
      }
    }
    #endif
    #endif

    #ifdef DEBUG_EN
    int thread_id = getWorkersID();
    #ifdef CHUNK
    if (d > 2) memory_profiler.edge_memory_usage[thread_id] += d * sizeof(uintE);
    #else
    memory_profiler.edge_memory_usage[thread_id] += d * sizeof(uintE);
    #endif
    edge_profiler.edge_access[thread_id] += d;
    if (!inGraph) edge_profiler.out_edge_access[thread_id] += d;
    else edge_profiler.in_edge_access[thread_id] += d;

    #ifdef VERTEXCUT_PROFILE_EN
    vertexcut_profiler.vertex_accessed[thread_id] += 1;
    vertexcut_profiler.check_vertexcut(neighbors, d, inGraph, thread_id);
    #endif
    #endif
    
    return neighbors;
  }

  inline bool isReorderListEnabled() {
    #ifdef CHUNK
    if (manager == NULL) return false;
    return manager->getReorderListEnable();
    #else
    return false;
    #endif
  }

  inline uintE* getReorderList(bool inGraph) {
    #ifdef CHUNK
    return manager->getReorderList(inGraph);
    #else
    return NULL;
    #endif
  }

  inline uintE getReorderID(bool inGraph, uintE i) {
    #ifdef CHUNK
    return manager->getReorderListElement(inGraph, i);
    #else
    return i;
    #endif
  }
};

#endif
