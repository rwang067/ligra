#pragma once

#include <sys/time.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <unordered_map>
#include <string>
#include <utility>

// #define PROFILE_EN
#define DEBUG_EN

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

// only the runtime memory usage is recorded
struct memory_profiler_t {
   #define MAX_THREAD_NUM 96
   // item, memory usage
   std::unordered_map<std::string, size_t> memory_usage;
   // memory usage of edge in chunk (multi-thread)
   size_t edge_memory_usage[MAX_THREAD_NUM] = {0};

   void print_memory_usage() {
      size_t total_memory_usage = 0;
      for (auto it = memory_usage.begin(); it != memory_usage.end(); ++it) {
         total_memory_usage += it->second;
      }
      for (int i = 0; i < MAX_THREAD_NUM; ++i) {
         total_memory_usage += edge_memory_usage[i];
      }
      printf("total_memory_usage = %.2lfGB\n", total_memory_usage / 1024.0 / 1024.0 / 1024.0);
   }

   void print_memory_usage_detail() {
      for (auto it = memory_usage.begin(); it != memory_usage.end(); ++it) {
         printf("\t%s = %.2lfGB\n", it->first.c_str(), it->second / 1024.0 / 1024.0 / 1024.0);
      }
      size_t total_edge_memory_usage = 0;
      for (int i = 0; i < MAX_THREAD_NUM; ++i) {
         total_edge_memory_usage += edge_memory_usage[i];
         // printf("edge_memory_usage[%d] = %.2lfGB\n", i, edge_memory_usage[i] / 1024.0 / 1024.0 / 1024.0);
      }
      printf("\tedge_memory_usage = %.2lfGB\n", total_edge_memory_usage / 1024.0 / 1024.0 / 1024.0);
   }
};

struct edge_profiler_t {
   #define MAX_THREAD_NUM 96
   
   // memory usage of edge in chunk (multi-thread)
   size_t edge_access[MAX_THREAD_NUM] = {0};
   size_t out_edge_access[MAX_THREAD_NUM] = {0};
   size_t in_edge_access[MAX_THREAD_NUM] = {0};

   void print_edge_access() {
      size_t total_edge_access = 0;
      for (int i = 0; i < MAX_THREAD_NUM; ++i) {
         total_edge_access += edge_access[i];
      }
      printf("total_edge_access = %lu\n", total_edge_access);
   }

   void print_out_edge_access() {
      size_t total_out_edge_access = 0;
      for (int i = 0; i < MAX_THREAD_NUM; ++i) {
         total_out_edge_access += out_edge_access[i];
      }
      printf("total_out_edge_access = %lu\n", total_out_edge_access);
   }

   void print_in_edge_access() {
      size_t total_in_edge_access = 0;
      for (int i = 0; i < MAX_THREAD_NUM; ++i) {
         total_in_edge_access += in_edge_access[i];
      }
      printf("total_in_edge_access = %lu\n", total_in_edge_access);
   }
};

struct stat_profiler_t {
   uint64_t _total_accessed_edges = 0;

   void print_total_accessed_edges() {
      printf("total_accessed_edges = %lu\n", _total_accessed_edges);
      printf("total_accessed_edges = %.2lfGB\n", _total_accessed_edges * sizeof(uintE) / 1024.0 / 1024.0 / 1024.0);
   }
};

#ifdef PROFILE_EN
profiler_t profiler = profiler_t(96);
#endif

#ifdef DEBUG_EN
memory_profiler_t memory_profiler;
edge_profiler_t edge_profiler;
stat_profiler_t stat_profiler;
#endif