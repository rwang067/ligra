#pragma once
#include <iostream>
#include <string>
#include <fstream>

#include "../src/chunkgraph.hpp"
#include "../src/monitor.hpp"

#include "pr.h"
#include "bfs.h"
#include "bc.h"
#include "cc.h"
#include "gapbs/pvector.h"

#include "breadth_first_search.hpp"
#include "pagerank.hpp"
#include "connected_components.hpp"

#include <sys/time.h>
#include <stdlib.h>
#include <vector>

typedef float ScoreT;

void test_gapbs_graph_benchmarks(ChunkGraph* qsnap){
    uint32_t count = qsnap->get_query_count();
    #pragma region test_gapbs_graph_benchmarks
    while (count--) {
        double start, end;
        // graph benchmark test with subgraph based NUMA optimization
        {   // test_gapbs_pr
        std::cout << "<<<<<<<<<ALGO: PAGERANK>>>>>>>>>" << std::endl;
        start = mywtime();
        pvector<ScoreT> pr_ret = run_pr(qsnap, 10);
        end = mywtime();
        std::cout << "PageRank time = " << (end - start) << std::endl;
        PrintTopPRScores(qsnap, pr_ret);
        // std::cout << "test_gapbs_bfs for " << root << " root vertex, sum of frontier count = " << bfs_ret.size() << ", BFS Time = " << end - start << std::endl;
        }
        {   // test_gapbs_bfs
        std::cout << "<<<<<<<<<ALGO: BFS>>>>>>>>>" << std::endl;
        start = mywtime();
        pvector<vid_t> bfs_ret = run_bfs(qsnap, qsnap->get_ecount(), qsnap->get_source_vertex());
        end = mywtime();
        PrintBFSStats(qsnap, bfs_ret);
        std::cout << "test_gapbs_bfs for " << qsnap->get_source_vertex() << " root vertex, sum of frontier count = " << bfs_ret.size() << ", BFS Time = " << end - start << std::endl;
        }
        { // test_gapbs_bc
        std::cout << "<<<<<<<<<ALGO: BC>>>>>>>>>" << std::endl;
        start = mywtime();
        pvector<ScoreT> bc_ret = run_bc(qsnap, qsnap->get_ecount(), qsnap->get_source_vertex(), 1);
        end = mywtime();
        std::cout << "BC time = " << (end - start) << std::endl;
        PrintTopScores(qsnap, bc_ret);
        }
        { // test_gapbs_cc-sv
        std::cout << "<<<<<<<<<ALGO: CC_SV>>>>>>>>>" << std::endl;
        start = mywtime();
        pvector<vid_t> cc_ret = run_cc(qsnap);
        end = mywtime();
        std::cout << "CC time = " << (end - start) << std::endl;
        PrintCompStats(qsnap, cc_ret);
        }
        std::cout << std::endl;
    }
#pragma endregion test_gapbs_graph_benchmarks
}


void test_graph_benchmarks(ChunkGraph* qsnap) {
    omp_set_num_threads(QUERY_THD_COUNT);
    ofs << "[QueryTimings]:";
    uint32_t count = qsnap->get_query_count();
    #pragma region test_graph_benchmarks
    while (count--) {
        double start, end;
        { // test_bfs
            vid_t query_roots = qsnap->get_source_vertex();
            start = mywtime();
            index_t res = test_bfs(qsnap, query_roots);
            // index_t res = test_bfs_iterative(qsnap, query_roots);
            end = mywtime();
            ofs << (end - start) << ",";
            ofs.flush();
            std::cout << "test_bfs for " << query_roots << ", sum of frontier count = " << res << ", BFS Time = " << end - start << std::endl;
        }
        query_id++;
        { // test_pagerank
            index_t num_iterations = 10;
            start = mywtime();
            test_pagerank_pull(qsnap, num_iterations);
            end = mywtime();
            ofs << (end - start) << ",";
            ofs.flush();
            std::cout << "test_pagerank_pull for " << num_iterations << " iterations, PageRank Time = " << end - start << std::endl;
        }
        query_id++;
        { // test_connect_component
            index_t neighbor_rounds = 2;
            start = mywtime();
            test_connected_components(qsnap, neighbor_rounds);
            end = mywtime();
            ofs << (end - start) << ",";
            ofs.flush();
            std::cout << "test_connect_component with neighbor_rounds = " << neighbor_rounds << ", Connect Component Time = " << end - start << std::endl;
        }
    }
    #pragma endregion test_graph_benchmarks
    query_record.print();
    ofs << std::endl;
    ofs.close();
}

void test_out_neighbors(ChunkGraph* qsnap) {
    vid_t query_list[6] = {12, 300, 1024, 65535, 1023512, 13757872};
    for (int i = 0; i < 6; ++i) {
        degree_t out_deg = qsnap->get_out_degree(query_list[i]);
        std::cout << "out degree of " << query_list[i] << " is " << out_deg << std::endl;
        if (out_deg == 0) continue;
        degree_t local_degree = 0;
        vid_t* local_adjlist = 0;
        local_adjlist = new vid_t[out_deg];
        local_degree = qsnap->get_out_nebrs(query_list[i], local_adjlist);
        assert(local_degree == out_deg);
        degree_t threshold = local_degree > 100 ? 100 : local_degree;
        std::cout << "The top " << threshold <<  " in neighbors of " << query_list[i] << " are: ";
        for (degree_t j = 0; j < threshold; ++j) {
            std::cout << local_adjlist[j] << " ";
        }
        std::cout << std::endl;
        delete [] local_adjlist;
    }
}

void test_in_neighbors(ChunkGraph* qsnap) {
    vid_t query_list[6] = {12, 300, 1024, 65535, 1023512, 13757872};
    for (int i = 0; i < 6; ++i) {
        degree_t out_deg = qsnap->get_in_degree(query_list[i]);
        std::cout << "in degree of " << query_list[i] << " is " << out_deg << std::endl;
        if(out_deg == 0) continue;
        degree_t local_degree = 0;
        vid_t* local_adjlist = 0;
        local_adjlist = new vid_t[out_deg];
        local_degree = qsnap->get_in_nebrs(query_list[i], local_adjlist);
        assert(local_degree == out_deg);
        degree_t threshold = local_degree > 100 ? 100 : local_degree;
        std::cout << "The top " << threshold <<  " in neighbors of " << query_list[i] << " are: ";
        for (degree_t j = 0; j < threshold; ++j) {
            std::cout << local_adjlist[j] << " ";
        }
        std::cout << std::endl;
        delete [] local_adjlist;
    }
}