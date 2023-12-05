#include "ligra.h"
#include <assert.h>

template <class vertex>
void printNebrs(graph<vertex>& G, int i, bool InGraph){
  uint d; uintE* nebrs;
  if(InGraph == 0){
    d = G.V[i].getOutDegree();
    // uintE* nebrs = G.V[i].getOutNeighbors();
    nebrs = G.getChunkNeighbors(&(G.V[i]),0);
    uint threshold = d <= 100 ? d : 100;
    cout << "i = " << i << ", outd = " << d << ", top " << threshold << " Nebrs = ";
    for(int j = 0; j < threshold; j++)
      cout << nebrs[j] << ", ";
    cout << "\n" << endl;
  } else {
    d = G.V[i].getInDegree();
    // nebrs = G.V[i].getInNeighbors();
    nebrs = G.getChunkNeighbors(&(G.V[i]),1);
    uint threshold = d <= 100 ? d : 100;
    cout << "i = " << i << ", ind = " << d << ", top " << threshold << " Nebrs = ";
    for(int j = 0; j < threshold; j++)
      cout << nebrs[j] << ", ";
    cout  << "\n" << endl;
  }
}

template <class vertex>
void Compute(graph<vertex>& GA, commandLine P) {
  // printNebrs(GA, 1, 0); // expect 20: 2~21 for FS
  // printNebrs(GA, 3, 0); // expect 4: 2 1 861 862 for FS
  // printNebrs(GA, 4, 0); // expect 1: 1 for FS
  // printNebrs(GA, 9, 0); // expect 1: 1 for FS
  // printNebrs(GA, 28, 0); // expect 2: 26 30 for FS
  // printNebrs(GA, 164, 0); // expect 2: 158 1488 for FS
  
  // printNebrs(GA, 2, 1); // expect 196: 1 22 26 ... 850 for FS-sub; expect 203: 1 3 12 22 26 ... 31839623 for FS
  // printNebrs(GA, 3, 1); // expect 3: 1 861 862 for FS

  // printNebrs(GA, 338093904, 0); // expect 6: 2805010, 510407612, 1349255, 223596394, 468811973, 31853317 for K29
  // printNebrs(GA, 493455215, 0); // expect 30388: 86302919~419861469~346750783 for K29

  printNebrs(GA, 12, 0);
  printNebrs(GA, 12, 1);
}

template <class vertex>
void Compare(graph<vertex>& GA1, graph<vertex>& GA2, commandLine P) {
  long start = P.getOptionLongValue("-r",0);
  long n = GA1.n;
  assert(n == GA2.n);

  std::cout << "GA1.n = " << GA1.n << std::endl;
  std::cout << "GA2.n = " << GA2.n << std::endl;

  std::cout << "start compare out edge " << std::endl;

  for (long i = 0; i < n; ++i) {
    uint deg1 = GA1.V[i].getOutDegree();
    uint deg2 = GA2.V[i].getOutDegree();
    if (deg1 != deg2) {
      std::cout << "vid = " << i << std::endl;
      uint d1 = printNebrs(GA1, i, 0);
      uint d2 = printNebrs(GA2, i, 0);
      exit(0);
    }
    uintE* nebrs1 = GA1.getChunkNeighbors(&(GA1.V[i]), 0);
    uintE* nebrs2 = GA2.getChunkNeighbors(&(GA2.V[i]), 0);
    
    for(long j = 0; j < deg1; ++j) {
      if(nebrs1[j] != nebrs2[j]) {
        uint d1 = printNebrs(GA1, i, 0);
        uint d2 = printNebrs(GA2, i, 0);
        exit(0);
      }
    }
  }
  std::cout << "finish compare out edge " << std::endl;

  std::cout << "start compare in edge " << std::endl;
  for (long i = 0; i < n; ++i) {
    uint deg1 = GA1.V[i].getInDegree();
    uint deg2 = GA2.V[i].getInDegree();
    if (deg1 != deg2) {
      std::cout << "vid = " << i << std::endl;
      uint d1 = printNebrs(GA1, i, 1);
      uint d2 = printNebrs(GA2, i, 1);
      exit(0);
    }
    uintE* nebrs1 = GA1.getChunkNeighbors(&(GA1.V[i]), 1);
    uintE* nebrs2 = GA2.getChunkNeighbors(&(GA2.V[i]), 1);
    
    for(long j = 0; j < deg1; ++j) {
      if(nebrs1[j] != nebrs2[j]) {
        uint d1 = printNebrs(GA1, i, 1);
        uint d2 = printNebrs(GA2, i, 1);
        exit(0);
      }
    }
  }
  std::cout << "finish compare in edge " << std::endl;
}