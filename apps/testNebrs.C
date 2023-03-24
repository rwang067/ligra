#include "ligra.h"

template <class vertex>
void printNebrs(graph<vertex>& G, int i, bool isInGraph){
  uint d; uintE* nebrs;
  if(isInGraph == 0){
    d = G.V[i].getOutDegree();
    // uintE* nebrs = G.V[i].getOutNeighbors();
    nebrs = G.getChunkNeighbors(&(G.V[i]),0);
    cout << "i = " << i << ", outd = " << d << ", Nebrs = ";
    for(int j = 0; j < d; j++)
      cout << nebrs[j] << ", ";
    cout << "\n" << endl;
  } else {
    d = G.V[i].getInDegree();
    // nebrs = G.V[i].getInNeighbors();
    nebrs = G.getChunkNeighbors(&(G.V[i]),1);
    cout << "i = " << i << ", ind = " << d << ", Nebrs = ";
    for(int j = 0; j < d; j++)
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

  printNebrs(GA, 338093904, 0); // expect 6: 2805010, 510407612, 1349255, 223596394, 468811973, 31853317 for K29
  printNebrs(GA, 493455215, 0); // expect 30388: 86302919~419861469~346750783 for K29
}
