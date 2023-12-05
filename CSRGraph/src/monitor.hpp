#pragma once
#include "common.hpp"
#include "args_config.hpp"

#define MAX_QUERY_ALGO 3
#define MAX_QUERY_THREAD 96

int query_id = 0;
cpos_t max_chunkID = 0;
cpos_t max_offset = 0;

std::string query_name[MAX_QUERY_ALGO] = {
    "BFS",
    "PageRank",
    "Connected Components"
};

struct query_record_t {
public:
    vid_t inplace_cnt[MAX_QUERY_ALGO];                                  // number of inplace vertices
    cpos_t visited_cnum[MAX_QUERY_ALGO];                                // number of visited chunks
    std::vector<cpos_t> visited_cpos[MAX_QUERY_ALGO];                   // visited chunks
    std::unordered_map<cpos_t, int> visited_cpos_map[MAX_QUERY_ALGO];   // visited chunks

    query_record_t() {
        for (int i = 0; i < MAX_QUERY_ALGO; ++i) {
            inplace_cnt[i] = 0;
            visited_cnum[i] = 0;
        }
    }

    void record_inplace() {
        inplace_cnt[query_id]++;
    }

    void record_chunk(cpos_t chunk_id) {
        if (visited_cpos_map[query_id].find(chunk_id) == visited_cpos_map[query_id].end()) {
            visited_cnum[query_id]++;
            visited_cpos_map[query_id][chunk_id] = 1;
        } else {
            visited_cpos_map[query_id][chunk_id]++;
        }
        visited_cpos[query_id].push_back(chunk_id);
    }

    void print() {
        std::ofstream monitor_file;
        monitor_file.open("monitor.txt", std::ios::app);
        
        for (int i = 0; i < MAX_QUERY_ALGO; ++i) {
            monitor_file << "Query " << query_name[i] << " visited " << visited_cnum[i] << " chunks" << std::endl;
            monitor_file << "Query " << query_name[i] << " visited " << visited_cpos[i].size() << " chunks: ";
            // for (auto cpos : visited_cpos[i]) {
            //     monitor_file << cpos << " ";
            // }
            monitor_file << std::endl;
        }
        monitor_file.close();
    }
};

struct thd_monitor_t {
public:
    vid_t inplace_cnt[MAX_QUERY_ALGO];                                  // number of inplace vertices
    cpos_t visited_cnum[MAX_QUERY_ALGO];                                // number of visited chunks
    cpos_t visited_cpos[MAX_QUERY_ALGO];                                // visited chunks
    query_record_t* query_record;


    thd_monitor_t() {
        query_record = new query_record_t[MAX_QUERY_THREAD];
        for (int i = 0; i < MAX_QUERY_ALGO; ++i) {
            inplace_cnt[i] = 0;
            visited_cnum[i] = 0;
            visited_cpos[i] = 0;
        }
    }

    ~thd_monitor_t() {
        delete[] query_record;
    }

    void record_inplace() {
        query_record[omp_get_thread_num()].record_inplace();
    }

    void record_chunk(cpos_t chunk_id) {
        query_record[omp_get_thread_num()].record_chunk(chunk_id);
    }

    void print() {
        std::ofstream monitor_file;
        monitor_file.open("monitor.txt", std::ios::app);
        monitor_file << "===============[JOB-" << JOB << "]===============" << std::endl;
        monitor_file << "Max chunk ID: " << max_chunkID << std::endl;
        for (int i = 0; i < MAX_QUERY_ALGO; ++i) {
            for (int j = 0; j < MAX_QUERY_THREAD; ++j) {
                inplace_cnt[i] += query_record[j].inplace_cnt[i];
                visited_cnum[i] += query_record[j].visited_cnum[i];
                visited_cpos[i] += query_record[j].visited_cpos[i].size();
            }
            monitor_file << "Query " << query_name[i] << " visited " << inplace_cnt[i] << " vertices in place" << std::endl;
            monitor_file << "Query " << query_name[i] << " visited " << visited_cnum[i] << " chunks" << std::endl;
            monitor_file << "Query " << query_name[i] << " visited " << visited_cpos[i] << " chunks in total" << std::endl;
        }
        monitor_file.close();
    }
};

#ifdef MONITOR
thd_monitor_t query_record;
#endif