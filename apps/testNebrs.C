#include "ligra.h"

template <class vertex>
void printNebrs(graph<vertex>& G, int i, bool isInGraph){
  uint d; uintE* nebrs;
  if(isInGraph == 0){
    d = G.V[i].getOutDegree();
    nebrs = (uintE*) G.V[i].getOutNeighbors();
    cout << "i = " << i << ", outd = " << d << ", Nebrs = ";
    for(int j = 0; j < d; j++)
      cout << nebrs[j] << ", ";
    cout << "\n" << endl;
  } else {
    d = G.V[i].getInDegree();
    nebrs = (uintE*) G.V[i].getInNeighbors();
    cout << "i = " << i << ", ind = " << d << ", Nebrs = ";
    for(int j = 0; j < d; j++)
      cout << nebrs[j] << ", ";
    cout  << "\n" << endl;
  }
}

template <class vertex>
void Compute(graph<vertex>& GA, commandLine P) {
  printNebrs(GA, 1, 0); // expect 20: 2~21 for FS
  printNebrs(GA, 3, 0); // expect 4: 2 1 861 862 for FS
  printNebrs(GA, 4, 0); // expect 1: 1 for FS
  
  printNebrs(GA, 2, 1); // expect 203: 1 3 12 22 26 ... 31839623 for FS; expect 196: 1 22 26 ... 850 for FS-sub
  printNebrs(GA, 3, 1); // expect 3: 1 861 862 for FS
}