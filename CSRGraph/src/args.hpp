#pragma once

#include "common.hpp"

/* ---------------------------------------------------------------------- */
// Basic arguments for graph info
std::string filepath; // path of graph dataset
std::string PREFIX; // prefix name of input csr file
uint32_t nverts;
uint64_t nedges;
/* ---------------------------------------------------------------------- */
// Basic arguments for system info
uint32_t JOB; // job type
uint32_t THD_COUNT; // num of threads
uint32_t QUERY_THD_COUNT; // num of threads
std::string SSDPATH; // path of ssd
// Basic arguments for graph query benchmark
uint32_t QUERY; // times for query
uint32_t source;
uint32_t reorder_level;
double global_threshold;
/* ---------------------------------------------------------------------- */
// Basic arguments for chunk allocator
size_t SBLK_POOL_SIZE; // size of ssd pool for chunk allocator
std::string sblk_name; // name of ssd pool for chunk allocator
uint32_t MAX_LEVEL;     // max level of chunk allocator
size_t MEM_BULK_SIZE;  // size of memory bulk for vertex allocator
std::ofstream ofs;     // output file stream for result