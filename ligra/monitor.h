#pragma once

#include <sys/time.h>
#include <stdlib.h>
#include <unistd.h>

// #define PROFILE_EN
// #define DEBUG_EN

class profiler_t {
public:
   profiler_t() {}
   profiler_t(int n) : nthreads(n) {
      num_load_chunk = (size_t*)calloc(n, sizeof(size_t));
      num_get_chunk = (size_t*)calloc(n, sizeof(size_t));
   }

   ~profiler_t() {
      if (num_load_chunk) free(num_load_chunk);
      if (num_get_chunk) free(num_get_chunk);
   }

   inline size_t get_num_load_chunk(int tid) { return num_load_chunk[tid]; }
   inline size_t get_num_get_chunk(int tid) { return num_get_chunk[tid]; }

   inline void profile_load_chunk(int tid) { num_load_chunk[tid]++; }
   inline void profile_load_chunk(int tid, uint32_t count) { num_load_chunk[tid] += count; }
   inline void profile_get_chunk(int tid) { num_get_chunk[tid]++; }

   inline void print_page_miss_ratio() {
      size_t sum_num_load_chunk = 0, sum_num_get_chunk = 0;
      for (int i = 0; i < nthreads; ++i) sum_num_load_chunk += num_load_chunk[i];
      for (int i = 0; i < nthreads; ++i) sum_num_get_chunk += num_get_chunk[i];
      
      printf("num_load_chunk = %lu, num_get_chunk = %lu, ratio = %.2f\n", sum_num_load_chunk, sum_num_get_chunk, (double)sum_num_load_chunk / (double)sum_num_get_chunk);
   }

private:
   size_t *num_load_chunk, *num_get_chunk;
   int nthreads = 0;
};

#ifdef PROFILE_EN
profiler_t profiler = profiler_t(96);
#endif