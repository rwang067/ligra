#pragma once
#include <stdint.h> //uint32_t
#include <stddef.h> //size_t
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <cmath>
#include <cstring> //memset + memcpy
#include <omp.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <algorithm>

#include "utils/metrics/cmdopts.hpp"
#include "utils/metrics/metrics.hpp"
#include "utils/logstream.hpp"

typedef uint32_t vid_t; //vertex id
typedef uint64_t index_t;
typedef uint32_t degree_t;
typedef uint32_t sid_t; //snap id
typedef uint32_t tid_t;
typedef uint64_t cpos_t;

#define MONITOR

/* ---------------------------------------------------------------------- */
#define KB      (1024)
#define MB      (1024*KB)
#define GB      (1024*MB)

#define ALIGN_SIZE (4 << 10) // 4KB
#define VERT_BULK_SIZE (1 << 20) // 1M

inline double mywtime()
{
	double time[2];	
	struct timeval time1;
	gettimeofday(&time1, NULL);

	time[0]=time1.tv_sec;
	time[1]=time1.tv_usec;

	return time[0] + time[1]*1.0e-6;
}