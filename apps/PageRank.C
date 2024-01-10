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
#include "ligra.h"
#include "math.h"

#define PR_DEBUG_EN

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

template <class vertex>
void Compute(graph<vertex>& GA, commandLine P) {
  long maxIters = P.getOptionLongValue("-maxiters",10);
  const intE n = GA.n;
  const double damping = 0.85, epsilon = 0.0000001;
  
  double one_over_n = 1/(double)n;
  double* p_curr = newA(double,n);
  {parallel_for(long i=0;i<n;i++) p_curr[i] = one_over_n;}
  double* p_next = newA(double,n);
  {parallel_for(long i=0;i<n;i++) p_next[i] = 0;} //0 if unchanged
  bool* frontier = newA(bool,n);
  {parallel_for(long i=0;i<n;i++) frontier[i] = 1;}

  vertexSubset Frontier(n,n,frontier);

#ifdef DEBUG_EN
  std::cout << "maxIters = " << maxIters << std::endl;
  std::string item = "Algo MetaData";
  memory_profiler.memory_usage[item] = 0;
  size_t size = sizeof(double) * n;  // p_curr, p_next
  memory_profiler.memory_usage[item] += size;
  memory_profiler.memory_usage[item] += size;
  size = sizeof(bool) * n;  // frontier
  memory_profiler.memory_usage[item] += size;

  size_t max_size = Frontier.getMemorySize();
#endif

#ifdef ITER_PROFILE_EN
  iteration_profiler.init_iostat();
#endif

  long iter = 0;
  double L1_norm = 1.0;

  while(iter++ < maxIters) {
#ifdef PR_DEBUG_EN
    time_t now = time(0);
    char* dt = ctime(&now);
    std::cout << "The local date and time is: " << dt;
#endif

#ifdef DEBUG_EN
    size_t vm, rss;
    pid_t pid = getpid();
    process_mem_usage(pid, vm, rss);
    std::cout << "iteration = " << iter << ", L1_norm_prev = " << L1_norm
              << "; memory usage: VM = " << B2GB(vm) << ", RSS = " << B2GB(rss);
    size = Frontier.getMemorySize();
    if (size > max_size) max_size = size;
#endif
    edgeMap(GA,Frontier,PR_F<vertex>(p_curr,p_next,GA.V),0, no_output);
    vertexMap(Frontier,PR_Vertex_F(p_curr,p_next,damping,n));
    //compute L1-norm between p_curr and p_next
    {parallel_for(long i=0;i<n;i++) {
      p_curr[i] = fabs(p_curr[i]-p_next[i]);
      }}
    L1_norm = sequence::plusReduce(p_curr,n);
    if(L1_norm < epsilon) break;
    //reset p_curr
    vertexMap(Frontier,PR_Vertex_Reset(p_curr));
    swap(p_curr,p_next);
#ifdef ITER_PROFILE_EN
    iteration_profiler.record_iostat();
#endif
#ifdef VERTEXCUT_PROFILE_EN
    vertexcut_profiler.record_vertexcut();
    vertexcut_profiler.record_vertex_accessed();
#endif
  }
#ifdef DEBUG_EN
    size_t vm, rss;
    pid_t pid = getpid();
    process_mem_usage(pid, vm, rss);
    std::cout << "iteration = " << iter << ", L1_norm_prev = " << L1_norm
              << "; memory usage: VM = " << B2GB(vm) << ", RSS = " << B2GB(rss) << std::endl;
    size = Frontier.getMemorySize();
    if (size > max_size) max_size = size;
#endif


#ifdef DEBUG_EN
  std::cout << "Frontier maximum memory usage = " << B2GB(max_size) << "GB" << std::endl;
  memory_profiler.memory_usage[item] += max_size;
#endif

  Frontier.del(); free(p_curr); free(p_next); 

#ifdef DEBUG_EN
  memory_profiler.print_memory_usage();
  memory_profiler.print_memory_usage_detail();

  edge_profiler.print_edge_access();
  edge_profiler.print_out_edge_access();
  edge_profiler.print_in_edge_access();

  stat_profiler.print_total_accessed_edges();
#endif

#ifdef VERTEXCUT_PROFILE_EN
  vertexcut_profiler.print_vertexcut_rate();
#endif
}
