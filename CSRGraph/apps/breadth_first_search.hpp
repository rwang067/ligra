#pragma once
#include <omp.h>
#include <random>
#include <algorithm>
#include <queue>
#include "../src/chunkgraph.hpp"

inline void print_bfs_summary(uint16_t* status, uint16_t level, vid_t v_count, vid_t root){
    vid_t sum = 0;
    for (int l = 1; l < level; ++l) {
        vid_t vid_count = 0;
        #pragma omp parallel for reduction (+:vid_count) 
        for (vid_t v = 0; v < v_count; ++v) {
            if (status[v] == l) ++vid_count;
            //if (status[v] == l && l == 3) cout << v << endl;
        }
        sum += vid_count;
        std::cout << " Level = " << l << " count = " << vid_count << std::endl;
    }
    std::cout << " bfs_summary of root " << root << " = " << sum << std::endl;
}

index_t test_bfs(ChunkGraph* qsnap, vid_t query_roots){
    std::cout << "test_bfs..." << std::endl;
    vid_t           v_count    = qsnap->get_vcount();

    srand(0);
    index_t total_count = 0;

    vid_t root = query_roots;

    int				level      = 1;
    int				top_down   = 1;
    vid_t			frontier   = 0;
    
    // double start1 = mywtime();
    uint16_t* status = new uint16_t[v_count];
    status[root] = level;
    
    total_count += 1; 
    do {
        frontier = 0;
        // double start = mywtime();
        #pragma omp parallel reduction(+:frontier)
        {
            vid_t vid;
            degree_t nebr_count = 0;
            degree_t local_degree = 0;
            vid_t* local_adjlist = 0;
            
            if (top_down) {
                #pragma omp for nowait
                for (vid_t v = 0; v < v_count; v++) {
                    if (status[v] != level) continue;
                    
                    nebr_count = qsnap->get_out_degree(v);
                    if (0 == nebr_count) continue;
                    
                    local_adjlist = new vid_t[nebr_count];
                    local_degree  = qsnap->get_out_nebrs(v, local_adjlist);
                    assert(local_degree  == nebr_count);
                    
                    //traverse the delta adj list
                    for (degree_t i = 0; i < local_degree; ++i) {
                        vid = local_adjlist[i];
                        if (status[vid] == 0) {
                            status[vid] = level + 1;
                            ++frontier;
                        }
                    }
                    delete [] local_adjlist;
                }
            } else { // bottom up
                #pragma omp for nowait
                for (vid_t v = 0; v < v_count; v++) {
                    if (status[v] != 0) continue;

                    nebr_count = qsnap->get_in_degree(v);
                    if (0 == nebr_count) continue;

                    local_adjlist = new vid_t[nebr_count];
                    local_degree  = qsnap->get_in_nebrs(v, local_adjlist);
                    assert(local_degree  == nebr_count);

                    for (degree_t i = 0; i < local_degree; ++i) {
                        vid = local_adjlist[i];
                        if (status[vid] == level) {
                            status[v] = level + 1;
                            ++frontier;
                            break;
                        }
                    }
                    delete [] local_adjlist;
                }
            }
        }
        
        // double end = mywtime();
        // std::cout << "Top down = " << top_down
        //      << " Level = " << level
        //      << " Frontier Count = " << frontier
        //      << " Time = " << end - start
        //      << std::endl;

        // Point is to simulate bottom up bfs, and measure the trade-off    
        if (frontier >= 0.002 * v_count) {
            top_down = false;
        } else {
            top_down = true;
        }
        ++level;
        total_count += frontier;
    } while (frontier);

    print_bfs_summary(status, level, v_count, root);
    delete [] status;
    
    return total_count;
		
    // double end1 = mywtime();
    // std::string statistic_filename = "hierg_query.csv";
    // std::ofstream ofs;
    // ofs.open(statistic_filename.c_str(), std::ofstream::out | std::ofstream::app );
    // ofs << "BFS root = "<< root << ", Time = " << end1 - start1 << ", total count = " << total_count << std::endl;
    // ofs.close();
    // // std::cout << "BFS root = "<< root << ", Time = " << end1 - start1 << ", total count = " << total_count << std::endl;
    // // print_bfs_summary(status, level, v_count);
    // return end1 - start1;
}

struct Sparse {
    vid_t* sparse;
    vid_t n;
    Sparse(vid_t* _sparse, vid_t _n) : sparse(_sparse), n(_n) {}
};

index_t test_bfs_sparse(ChunkGraph* qsnap, vid_t query_roots){
    std::cout << "test_bfs..." << std::endl;
    vid_t           v_count    = qsnap->get_vcount();

    srand(0);
    index_t total_count = 0;

    vid_t root = query_roots;

    int				level      = 1;
    int				top_down   = 1;
    vid_t			frontier   = 0;
    
    // double start1 = mywtime();
    uint16_t* status = new uint16_t[v_count];
    status[root] = level;
    Sparse* S = new Sparse(new vid_t[v_count], 1);
    S->sparse[0] = root;

    total_count += 1; 
    do {
        frontier = 0;
        // double start = mywtime();
        #pragma omp parallel reduction(+:frontier)
        {   
            if (top_down) {
                #pragma omp for schedule(dynamic) nowait
                for (vid_t i = 0; i < S->n; i++) {
                    vid_t v = S->sparse[i];
                    
                    degree_t nebr_count = qsnap->get_out_degree(v);
                    if (0 == nebr_count) continue;
                    
                    vid_t* local_adjlist = new vid_t[nebr_count];
                    degree_t local_degree  = qsnap->get_out_nebrs(v, local_adjlist);
                    assert(local_degree  == nebr_count);
                    
                    //traverse the delta adj list
                    for (degree_t i = 0; i < local_degree; ++i) {
                        vid_t vid = local_adjlist[i];
                        if (status[vid] == 0) {
                            status[vid] = level + 1;
                            ++frontier;
                        }
                    }
                    delete [] local_adjlist;
                }
            } else { // bottom up
                #pragma omp for schedule(dynamic) nowait
                for (vid_t v = 0; v < v_count; v++) {
                    if (status[v] != 0) continue;

                    degree_t nebr_count = qsnap->get_in_degree(v);
                    if (0 == nebr_count) continue;

                    vid_t* local_adjlist = new vid_t[nebr_count];
                    degree_t local_degree  = qsnap->get_in_nebrs(v, local_adjlist);
                    assert(local_degree  == nebr_count);

                    for (degree_t i = 0; i < local_degree; ++i) {
                        vid_t vid = local_adjlist[i];
                        if (status[vid] == level) {
                            status[v] = level + 1;
                            ++frontier;
                            break;
                        }
                    }
                    delete [] local_adjlist;
                }
            }
        }
        
        // double end = mywtime();
        // std::cout << "Top down = " << top_down
        //      << " Level = " << level
        //      << " Frontier Count = " << frontier
        //      << " Time = " << end - start
        //      << std::endl;

        // Point is to simulate bottom up bfs, and measure the trade-off    

        if (frontier >= 0.002 * v_count) {
            top_down = false;
        } else {
            top_down = true;
        }
        ++level;
        total_count += frontier;
    } while (frontier);

    print_bfs_summary(status, level, v_count, root);
    delete [] status;
    
    return total_count;
		
    // double end1 = mywtime();
    // std::string statistic_filename = "hierg_query.csv";
    // std::ofstream ofs;
    // ofs.open(statistic_filename.c_str(), std::ofstream::out | std::ofstream::app );
    // ofs << "BFS root = "<< root << ", Time = " << end1 - start1 << ", total count = " << total_count << std::endl;
    // ofs.close();
    // // std::cout << "BFS root = "<< root << ", Time = " << end1 - start1 << ", total count = " << total_count << std::endl;
    // // print_bfs_summary(status, level, v_count);
    // return end1 - start1;
}

index_t test_bfs_iterative(ChunkGraph* qsnap, vid_t query_roots) {
    std::cout << "test_bfs_iterative..." << std::endl;
    vid_t           v_count    = qsnap->get_vcount();

    index_t total_count = 0;

    vid_t root = query_roots;

    int level = 1;
    
    uint16_t* visitied = new uint16_t[v_count];
    // use a queue to store active vertices
    std::queue<vid_t> active_vertices;
    active_vertices.push(root);
    visitied[root] = level;
    total_count += 1;

    while (true) {
        vid_t frontier = 0;
        vid_t step = active_vertices.size();
        
        for (vid_t i = 0; i < step; ++i) {
            vid_t v = active_vertices.front();
            active_vertices.pop();
            
            degree_t nebr_count = qsnap->get_out_degree(v);
            if (0 == nebr_count) continue;
            vid_t* local_adjlist = new vid_t[nebr_count];
            degree_t local_degree  = qsnap->get_out_nebrs(v, local_adjlist);
            assert(local_degree == nebr_count);

            for (degree_t d = 0; d < local_degree; ++d) {
                vid_t vid = local_adjlist[d];
                if (visitied[vid] == 0) {
                    visitied[vid] = level + 1;
                    ++frontier;
                    active_vertices.push(vid);
                }
            }
        }
        if (frontier == 0) break;
        ++level;
        total_count += frontier;
    }
    print_bfs_summary(visitied, level, v_count, root);
    delete [] visitied;
    return total_count;
}