#include "ligra.h"
#include "math.h"

struct BFS_F {
  uintE* Parents;
  BFS_F(uintE* _Parents) : Parents(_Parents) {}
  inline bool update (uintE s, uintE d) { //Update
    if(Parents[d] == UINT_E_MAX) { Parents[d] = s; return 1; }
    else return 0;
  }
  inline bool updateAtomic (uintE s, uintE d){ //atomic version of Update
    return (CAS(&Parents[d],UINT_E_MAX,s));
  }
  //cond function checks if vertex has been visited yet
  inline bool cond (uintE d) { return (Parents[d] == UINT_E_MAX); } 
};

template <class vertex>
struct PR_F {
  double* p_curr, *p_next;
  vertex* V;
  PR_F(double* _p_curr, double* _p_next, vertex* _V) : 
    p_curr(_p_curr), p_next(_p_next), V(_V) {}
  inline bool update(uintE s, uintE d){ //update function applies PageRank equation
    p_next[d] += p_curr[s]/V[s].getOutDegree();
    return 1;
  }
  inline bool updateAtomic (uintE s, uintE d) { //atomic Update
    writeAdd(&p_next[d],p_curr[s]/V[s].getOutDegree());
    return 1;
  }
  inline bool cond (intT d) { return cond_true(d); }};

//vertex map function to update its p value according to PageRank equation
struct PR_Vertex_F {
  double damping;
  double addedConstant;
  double* p_curr;
  double* p_next;
  PR_Vertex_F(double* _p_curr, double* _p_next, double _damping, intE n) :
    p_curr(_p_curr), p_next(_p_next), 
    damping(_damping), addedConstant((1-_damping)*(1/(double)n)){}
  inline bool operator () (uintE i) {
    p_next[i] = damping*p_next[i] + addedConstant;
    return 1;
  }
};

//resets p
struct PR_Vertex_Reset {
  double* p_curr;
  PR_Vertex_Reset(double* _p_curr) :
    p_curr(_p_curr) {}
  inline bool operator () (uintE i) {
    p_curr[i] = 0.0;
    return 1;
  }
};

struct CC_F {
  uintE* IDs, *prevIDs;
  CC_F(uintE* _IDs, uintE* _prevIDs) : 
    IDs(_IDs), prevIDs(_prevIDs) {}
  inline bool update(uintE s, uintE d){ //Update function writes min ID
    uintE origID = IDs[d];
    if(IDs[s] < origID) {
      IDs[d] = min(origID,IDs[s]);
      if(origID == prevIDs[d]) return 1;
    } return 0; }
  inline bool updateAtomic (uintE s, uintE d) { //atomic Update
    uintE origID = IDs[d];
    return (writeMin(&IDs[d],IDs[s]) && origID == prevIDs[d]);
  }
  inline bool cond (uintE d) { return cond_true(d); } //does nothing
};

//function used by vertex map to sync prevIDs with IDs
struct CC_Vertex_F {
  uintE* IDs, *prevIDs;
  CC_Vertex_F(uintE* _IDs, uintE* _prevIDs) :
    IDs(_IDs), prevIDs(_prevIDs) {}
  inline bool operator () (uintE i) {
    prevIDs[i] = IDs[i];
    return 1; }};

struct Update_Deg {
  intE* Degrees;
  Update_Deg(intE* _Degrees) : Degrees(_Degrees) {}
  inline bool update (uintE s, uintE d) { 
    Degrees[d]--;
    return 1;
  }
  inline bool updateAtomic (uintE s, uintE d){
    writeAdd(&Degrees[d],-1);
    return 1;
  }
  inline bool cond (uintE d) { return Degrees[d] > 0; }
};

template<class vertex>
struct Deg_LessThan_K {
  vertex* V;
  uintE* coreNumbers;
  intE* Degrees;
  uintE k;
  Deg_LessThan_K(vertex* _V, intE* _Degrees, uintE* _coreNumbers, uintE _k) : 
    V(_V), k(_k), Degrees(_Degrees), coreNumbers(_coreNumbers) {}
  inline bool operator () (uintE i) {
    if(Degrees[i] < k) { coreNumbers[i] = k-1; Degrees[i] = 0; return true; }
    else return false;
  }
};

template<class vertex>
struct Deg_AtLeast_K {
  vertex* V;
  intE *Degrees;
  uintE k;
  Deg_AtLeast_K(vertex* _V, intE* _Degrees, uintE _k) : 
    V(_V), k(_k), Degrees(_Degrees) {}
  inline bool operator () (uintE i) {
    return Degrees[i] >= k;
  }
};

template <class vertex>
void Compute(graph<vertex>& GA, commandLine P) {
    setWorkers(96);
    long n = GA.n;

    // std::cout << "=======1HOP=======" << std::endl;
    // srand(time(NULL));
    // long num = P.getOptionLongValue("-num",1L << 24);
    // long* samples = newA(long, num);
    // for(int i = 0; i < num; i++)
    // {
    //     long v = rand() * n / RAND_MAX;
    //     while(!GA.V[v].getOutDegree() && !GA.V[v].getInDegree())
    //         v = rand() * n / RAND_MAX;
    //     samples[i] = v;
    // }
    // startTime();
    // long deg_sum = 0;
    // {parallel_for(long i=0;i<num;i++) {
    //     long out_deg = GA.V[samples[i]].getOutDegree(), in_deg = GA.V[samples[i]].getInDegree();
    //     if(out_deg) {
    //         uintE* out_buff = newA(uintE, out_deg);
    //         memcpy(out_buff, GA.V[samples[i]].getOutNeighbors(), out_deg);
    //         free(out_buff);
    //     }
    //     if(in_deg) {
    //         uintE* in_buff = newA(uintE, in_deg);
    //         memcpy(in_buff, GA.V[samples[i]].getInNeighbors(), in_deg);
    //         free(in_buff);
    //     }
    //     deg_sum += out_deg + in_deg;
    // }}
    // double time = nextTime("Running time");
    // reportTimeToFile(time);
    // free(samples);
    
    std::cout << "=======BFS=======" << std::endl;
    startTime();
    long start = P.getOptionLongValue("-r",0);
    // long start = rounds == 4 ? P.getOptionLongValue("-r3",0) : (rounds == 3 ? P.getOptionLongValue("-r2",0) : P.getOptionLongValue("-r1",0));
    //creates Parents array, initialized to all -1, except for start
    uintE* Parents = newA(uintE,n);
    parallel_for(long i=0;i<n;i++) Parents[i] = UINT_E_MAX;
    Parents[start] = start;
    vertexSubset Frontier_BFS(n,start); //creates initial frontier
    while(!Frontier_BFS.isEmpty()){ //loop until frontier is empty
        vertexSubset output = edgeMap(GA, Frontier_BFS, BFS_F(Parents));    
        Frontier_BFS.del();
        Frontier_BFS = output; //set new frontier
    } 
    Frontier_BFS.del();
    free(Parents);
    double time = nextTime("Running time");
    reportTimeToFile(time);
    
    std::cout << "=======PageRank=======" << std::endl;
    startTime();
    long maxIters = P.getOptionLongValue("-maxiters",10);
    const double damping = 0.85, epsilon = 0.0000001;
    double one_over_n = 1/(double)n;
    double* p_curr = newA(double,n);
    {parallel_for(long i=0;i<n;i++) p_curr[i] = one_over_n;}
    double* p_next = newA(double,n);
    {parallel_for(long i=0;i<n;i++) p_next[i] = 0;} //0 if unchanged
    bool* frontier = newA(bool,n);
    {parallel_for(long i=0;i<n;i++) frontier[i] = 1;}
    vertexSubset Frontier_PR(n,n,frontier);
    long iter = 0;
    while(iter++ < maxIters) {
        edgeMap(GA,Frontier_PR,PR_F<vertex>(p_curr,p_next,GA.V),0, no_output);
        vertexMap(Frontier_PR,PR_Vertex_F(p_curr,p_next,damping,n));
        //compute L1-norm between p_curr and p_next
        {parallel_for(long i=0;i<n;i++) {
        p_curr[i] = fabs(p_curr[i]-p_next[i]);
        }}
        double L1_norm = sequence::plusReduce(p_curr,n);
        if(L1_norm < epsilon) break;
        //reset p_curr
        vertexMap(Frontier_PR,PR_Vertex_Reset(p_curr));
        swap(p_curr,p_next);
    }
    Frontier_PR.del(); free(p_curr); free(p_next);
    time = nextTime("Running time");
    reportTimeToFile(time);
    
    std::cout << "=======CC=======" << std::endl;
    startTime();
    uintE* IDs = newA(uintE,n), *prevIDs = newA(uintE,n);
    {parallel_for(long i=0;i<n;i++) IDs[i] = i;} //initialize unique IDs
    bool* fron = newA(bool,n);
    {parallel_for(long i=0;i<n;i++) fron[i] = 1;} 
    vertexSubset FrontierCC(n,n,fron); //initial frontier contains all vertices
    while(!FrontierCC.isEmpty()){ //iterate until IDS converge
        vertexMap(FrontierCC,CC_Vertex_F(IDs,prevIDs));
        vertexSubset output = edgeMap(GA, FrontierCC, CC_F(IDs,prevIDs));
        FrontierCC.del();
        FrontierCC = output;
    }
    FrontierCC.del(); free(IDs); free(prevIDs);
    time = nextTime("Running time");
    reportTimeToFile(time);

    std::cout << "=======KCore=======" << std::endl;
    startTime();
    bool* active = newA(bool,n);
    {parallel_for(long i=0;i<n;i++) active[i] = 1;}
    vertexSubset Frontier(n, n, active);
    uintE* coreNumbers = newA(uintE,n);
    intE* Degrees = newA(intE,n);
    {parallel_for(long i=0;i<n;i++) {
        coreNumbers[i] = 0;
        Degrees[i] = GA.V[i].getOutDegree();
    }}
    long largestCore = -1;
    long lim = n > 10 ? 10 : n;
    for (long k = 1; k <= lim; k++) {
      while (true) {
        vertexSubset toRemove 
    = vertexFilter(Frontier,Deg_LessThan_K<vertex>(GA.V,Degrees,coreNumbers,k));
        vertexSubset remaining = vertexFilter(Frontier,Deg_AtLeast_K<vertex>(GA.V,Degrees,k));
        Frontier.del();
        Frontier = remaining;
        if (0 == toRemove.numNonzeros()) { // fixed point. found k-core
    toRemove.del();
          break;
        }
        else {
    edgeMap(GA,toRemove,Update_Deg(Degrees), -1, no_output);
    toRemove.del();
        }
      }
      if(Frontier.numNonzeros() == 0) { largestCore = k-1; break; }
    }
    cout << "largestCore was " << largestCore << endl;
    Frontier.del(); free(coreNumbers); free(Degrees);
    time = nextTime("Running time");
    reportTimeToFile(time);
    std::cout << std::endl;
}
