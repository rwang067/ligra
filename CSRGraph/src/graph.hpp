#pragma once
#include "vertex.hpp"
#include "chunk_allocator.hpp"
#include "monitor.hpp"

class graph_t {
protected:
    // meta data
    vertex_t** vertices;
    bool is_out_graph;
    // memory pool
    thd_mem_t* thd_mem;
public:
    graph_t() {
        this->vertices = (vertex_t**)calloc(sizeof(vertex_t*), nverts);
        this->thd_mem = new thd_mem_t();
    }
    virtual ~graph_t() {
        if (vertices) free(vertices);
        delete thd_mem;
    }
    inline vertex_t* get_vertex(vid_t vid) { return vertices[vid]; }
    inline void set_vertex(vid_t vid, vertex_t* vert) { vertices[vid] = vert; }
    inline vertex_t* new_vertex() { return thd_mem->new_vertex(); }

    inline vid_t get_vcount() { return nverts; }
    inline index_t get_ecount() { return nedges; }

    virtual inline degree_t get_out_degree(vid_t vid) = 0;
    virtual inline degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) = 0;

    void save_vertices() {
        std::string filename = SSDPATH + "/" + PREFIX + ".vertex";
        size_t size = nverts * sizeof(vertex_t) * 2;
        int fd = open(filename.c_str(), O_RDWR|O_CREAT, 00777);
        if (fd == -1) {
            std::cout << "Could not open file for :" << filename << " error: " << strerror(errno) << std::endl;
            exit(1);
        }
        if (ftruncate(fd, size) == -1) {
            std::cout << "Could not truncate file for :" << filename << " error: " << strerror(errno) << std::endl;
            exit(1);
        }
        
        vertex_t* addr = (vertex_t*)mmap(NULL, size, PROT_READ|PROT_WRITE,MAP_SHARED, fd, 0);
        if (is_out_graph) {
            #pragma omp parallel for num_threads(THD_COUNT) schedule (dynamic, 256*256)
            for (vid_t vid = 0; vid < nverts; ++vid) {
                degree_t out_deg = 0;
                uint64_t residue = 0;
                vertex_t* vert = get_vertex(vid);
                if (vert) {
                    out_deg = vert->get_out_degree();
                    residue = vert->get_cpos();
                }
                addr[vid].out_deg = out_deg;
                addr[vid].residue = residue;
            }
        } else {
            #pragma omp parallel for num_threads(THD_COUNT) schedule (dynamic, 256*256)
            for (vid_t vid = 0; vid < nverts; ++vid) {
                degree_t out_deg = 0;
                uint64_t residue = 0;
                vertex_t* vert = get_vertex(vid);
                if (vert) {
                    out_deg = vert->get_out_degree();
                    residue = vert->get_cpos();
                }
                addr[vid+nverts].out_deg = out_deg;
                addr[vid+nverts].residue = residue;
            }
            // test case for vid = 12
            std::cout << "test case" << std::endl;
            std::cout << "out degree of " << 12 << " is " << addr[12].out_deg << std::endl;
            std::cout << "in degree of " << 12 << " is " << addr[12+nverts].out_deg << std::endl;
            
        }
        msync(addr, size, MS_SYNC);
        munmap(addr, size);
        close(fd);
    }
};


class csrgraph_t : public graph_t {
private:
    // graph data
    index_t* csr_idx;
    vid_t* csr_adj;
    size_t size_idx = 0;
    size_t size_adj = 0;

    // degree info
    degree_t max_degree = 0;
    degree_t threshold = 12;
    vid_t max_count = 10;
    vid_t* V_deg_bucket = NULL;
    index_t* E_deg_bucket = NULL;
    std::vector <vid_t>* degree_list;

public:
    csrgraph_t(vid_t nverts, index_t nedges, bool is_out_graph) : graph_t() {
        this->is_out_graph = is_out_graph;
        this->V_deg_bucket = (vid_t*)calloc(sizeof(vid_t), threshold+2);
        this->E_deg_bucket = (index_t*)calloc(sizeof(index_t), threshold+2);
        this->degree_list = new std::vector <vid_t>[threshold+2];
    }

    ~csrgraph_t() {
        char* buf_idx = (char*)csr_idx, *buf_adj = (char*)csr_adj;
        if (is_out_graph) {
            if(buf_idx) free_local_buf(size_idx, &buf_idx, "idx");
            if(buf_adj) free_local_buf(size_adj, &buf_adj, "adj");
        } else {
            if(buf_idx) free_local_buf(size_idx, &buf_idx, "ridx");
            if(buf_adj) free_local_buf(size_adj, &buf_adj, "radj");
        }
    }

    void import_csr() {
        char* buf_idx = 0, *buf_adj = 0;

        if (is_out_graph) {
            std::cout << "import csr for out graph" << std::endl;
            // allocate and read for index and csr file
            size_idx = alloc_and_read_file(filepath + "/" + PREFIX + ".idx", &buf_idx);
            size_adj = alloc_and_read_file(filepath + "/" + PREFIX + ".adj", &buf_adj);
            // get nverts and nedges
            nverts = size_idx / sizeof(index_t) - 1;
            nedges = size_adj / sizeof(vid_t);
            std::cout << "nverts = " << nverts << ", nedges = " << nedges  << ", average degree = " << nedges * 1.0 / nverts << std::endl;

            csr_idx = (index_t*)buf_idx;
            csr_adj = (vid_t*)buf_adj;
        } else {
            std::cout << "import csr for in graph" << std::endl;
            size_idx = alloc_and_read_file(filepath + "/" + PREFIX + ".ridx", &buf_idx, (nverts+1)*sizeof(index_t));
            size_adj = alloc_and_read_file(filepath + "/" + PREFIX + ".radj", &buf_adj);

            csr_idx = (index_t*)buf_idx;
            csr_adj = (vid_t*)buf_adj;
            csr_idx[nverts] = nedges;
        }

        #pragma omp parallel for num_threads(THD_COUNT) schedule (dynamic, 256*256)
        for (vid_t vid = 0; vid < nverts; ++vid) {
            degree_t deg = csr_idx[vid+1] - csr_idx[vid];
            if (deg) {
                vertex_t* vert = vertices[vid];
                vert = new_vertex();
                set_vertex(vid, vert);
                vert->set_out_degree(deg);
                vert->set_csr(csr_adj+csr_idx[vid]);
            }
        }
    }

    inline void count_degree() {
        char* buf_idx = 0, *buf_adj = 0;

        if (is_out_graph) {
            std::cout << "import csr for out graph" << std::endl;
            // allocate and read for index and csr file
            size_idx = alloc_and_read_file(filepath + "/" + PREFIX + ".idx", &buf_idx);
            size_adj = alloc_and_read_file(filepath + "/" + PREFIX + ".adj", &buf_adj);
            // get nverts and nedges
            nverts = size_idx / sizeof(index_t) - 1;
            nedges = size_adj / sizeof(vid_t);
            std::cout << "nverts = " << nverts << ", nedges = " << nedges  << ", average degree = " << nedges * 1.0 / nverts << std::endl;

            csr_idx = (index_t*)buf_idx;
            csr_adj = (vid_t*)buf_adj;
        } else {
            std::cout << "import csr for in graph" << std::endl;
            size_idx = alloc_and_read_file(filepath + "/" + PREFIX + ".ridx", &buf_idx, (nverts+1)*sizeof(index_t));
            size_adj = alloc_and_read_file(filepath + "/" + PREFIX + ".radj", &buf_adj);

            csr_idx = (index_t*)buf_idx;
            csr_adj = (vid_t*)buf_adj;
            csr_idx[nverts] = nedges;
        }
        
        degree_t D = (1 << threshold);
        for (vid_t vid = 0; vid < nverts; ++vid) {
            degree_t deg = csr_idx[vid+1] - csr_idx[vid];
            if (deg > max_degree) max_degree = deg;
            if (deg == 0) {
                V_deg_bucket[0]++;
                E_deg_bucket[0] += deg;
                if (degree_list[0].size() < max_count) degree_list[0].push_back(vid);
            } else if (deg < D) {
                int level = floor(log2(deg))+1;
                V_deg_bucket[level]++;
                E_deg_bucket[level] += deg;
                if (degree_list[level].size() < max_count) degree_list[level].push_back(vid);
            } else {
                V_deg_bucket[threshold+1]++;
                E_deg_bucket[threshold+1] += deg;
                if (degree_list[threshold+1].size() < max_count) degree_list[threshold+1].push_back(vid);
            }
        }
        std::cout << V_deg_bucket[threshold+1] << std::endl;
    }

    inline void print_degree() {
        degree_t D = (1 << threshold);
        vid_t vnz = nverts - V_deg_bucket[0];
        vid_t vaccumulate = 0;
        index_t eaccumulate = 0;

        if (is_out_graph) std::cout << "====================out graph====================" << std::endl;
        else std::cout << "====================in graph====================" << std::endl;
        std::cout << "max degree = " << max_degree << std::endl;

        printf("%-20s\t%-10s\t%-10s\t%-10s\t%-10s\t%-10s\t%-10s\t%-10s\n", "degree", "N(v)", "P(v)", "PNZ(v)", "A(v)", "N(e)", "P(e)", "A(e)");

        printf("%-20s\t%-10u\t%-10.2f\t%-10.2f\t%-10.2f\t%-10lu\t%-10.2f\t%-10.2f\n", 
            "[0, 1)", 
            V_deg_bucket[0], V_deg_bucket[0] * 100.0 / nverts, 0.0, 0.0, 
            E_deg_bucket[0], E_deg_bucket[0] * 100.0 / nedges, eaccumulate * 100.0 / nedges);

        for (degree_t i = 1; i < threshold+1; ++i) {
            vaccumulate += V_deg_bucket[i];
            eaccumulate += E_deg_bucket[i];
            printf("%-20s\t%-10u\t%-10.2f\t%-10.2f\t%-10.2f\t%-10lu\t%-10.2f\t%-10.2f\n", 
                ("[" + std::to_string(1 << (i-1)) + ", " + std::to_string(1 << i) + ")").c_str(), 
                V_deg_bucket[i], V_deg_bucket[i] * 100.0 / nverts, V_deg_bucket[i] * 100.0 / vnz, vaccumulate * 100.0 / vnz, 
                E_deg_bucket[i], E_deg_bucket[i] * 100.0 / nedges, eaccumulate * 100.0 / nedges);
        }
        vaccumulate += V_deg_bucket[threshold+1];
        eaccumulate += E_deg_bucket[threshold+1];
        printf("%-20s\t%-10u\t%-10.2f\t%-10.2f\t%-10.2f\t%-10lu\t%-10.2f\t%-10.2f\n", 
            ("[" + std::to_string(D) + ", inf)").c_str(), 
            V_deg_bucket[threshold+1], V_deg_bucket[threshold+1] * 100.0 / nverts, V_deg_bucket[threshold+1] * 100.0 / vnz, vaccumulate * 100.0 / vnz, 
            E_deg_bucket[threshold+1], E_deg_bucket[threshold+1] * 100.0 / nedges, eaccumulate * 100.0 / nedges);
        printf("%-20s\t%-10u\t%-10.2f\t%-10.2f\t%-10.2f\t%-10lu\t%-10.2f\t%-10.2f\n", 
            "total", nverts, 100.0, 100.0, 100.0, nedges, 100.0, 100.0);
        std::cout << nverts << " " << nedges << std::endl;
        std::cout << vaccumulate << " " << eaccumulate << std::endl;

        std::cout << "====================top 10====================" << std::endl;
        printf("%-20s\n", "degree");
        printf("%-20s\t", "[0, 1):");
        for (vid_t i = 0; i < degree_list[0].size(); ++i) {
            printf("%d ", degree_list[0][i]);
        }
        printf("\n");
        for (degree_t i = 1; i < threshold+1; ++i) {
            printf("%-20s\t", ("[" + std::to_string(1 << (i-1)) + ", " + std::to_string(1 << i) + "):").c_str());
            for (vid_t j = 0; j < degree_list[i].size(); ++j) {
                printf("%d ", degree_list[i][j]);
            }
            printf("\n");
        }
        printf("%-20s\t", ("[" + std::to_string(D) + ", inf):").c_str());
        for (vid_t i = 0; i < degree_list[threshold+1].size(); ++i) {
            printf("%d ", degree_list[threshold+1][i]);
        }
        printf("\n");
        std::cout << "====================end====================" << std::endl;
    }

    inline degree_t get_out_degree(vid_t vid) { 
        if (vertices[vid] == NULL) return 0;
        return vertices[vid]->get_out_degree(); 
    }

    inline degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { 
        if (vertices[vid] == NULL) return 0;
        degree_t degree = vertices[vid]->get_out_degree();
        vid_t* csr = vertices[vid]->get_csr();
        for (degree_t i = 0; i < degree; ++i) {
            nebrs[i] = csr[i];
        }
        return degree;
    }
};

class csrchunk_graph_t : public graph_t {
private:
    // chunk allocator
    chunk_allocator_csrchunk_t* chunk_allocator;

public:
    csrchunk_graph_t(vid_t nverts, index_t nedges, bool is_out_graph) : graph_t() {
        this->is_out_graph = is_out_graph;
        this->chunk_allocator = new chunk_allocator_csrchunk_t(is_out_graph);
    }

    ~csrchunk_graph_t() {
        delete chunk_allocator;
    }

    inline void init_chunk_allocator() { 
        std::string pool_name = sblk_name;
        if (is_out_graph) pool_name += "_out_";
        else pool_name += "_in_";
        chunk_allocator->init(pool_name);
    }

    void convert_graph(index_t* csr_idx, vid_t* csr_adj) {
        #ifdef MONITOR
        degree_t nzvcount = 0;
        #endif

        omp_set_num_threads(THD_COUNT);
        std::cout << "max threads = " << omp_get_max_threads() << std::endl;

        #pragma omp parallel for num_threads(THD_COUNT) schedule (dynamic, 256*256)
        for (vid_t vid = 0; vid < nverts; ++vid) {
            degree_t deg = csr_idx[vid+1] - csr_idx[vid];
            if (deg) {
                #ifdef MONITOR
                ++nzvcount;
                #endif
                vertex_t* vert = vertices[vid];
                vert = new_vertex();
                set_vertex(vid, vert);

                vert->set_out_degree(deg);

                // allocate vertex to a chunk
                vid_t* adjlist = NULL;
                cpos_t cpos = chunk_allocator->allocate(deg, &adjlist);
                vert->set_cpos(cpos);
                // copy adjlist to chunk
                memcpy(adjlist, csr_adj+csr_idx[vid], deg*sizeof(vid_t));

                // if (vid == 12)
                // {
                //     std::cout << "test case" << std::endl;
                //     std::cout << "degree of " << vid << " is " << deg << std::endl;
                //     degree_t threshold = deg > 100 ? 100 : deg;
                //     std::cout << "The top " << threshold <<  " out neighbors of " << vid << " are: ";
                //     for (degree_t i = 0; i < threshold; ++i) {
                //         std::cout << adjlist[i] << " ";
                //     }
                //     std::cout << std::endl;
                //     std::cout << "position of chunk = " << cpos << std::endl;
                //     printf("address of adjlist = %p\n", adjlist);
                // }
            }
        }
        #ifdef MONITOR
        max_chunkID = chunk_allocator->get_max_chunkID();
        std::cout << "max_chunkID = " << max_chunkID << std::endl;
        std::cout << "The number of non-zero degree vertices: " << nzvcount << std::endl;
        #endif
    }

    inline degree_t get_out_degree(vid_t vid) { 
        if (vertices[vid] == NULL) return 0;
        return vertices[vid]->get_out_degree(); 
    }

    inline degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { 
        if (vertices[vid] == NULL) return 0;
        degree_t degree = vertices[vid]->get_out_degree();
        vid_t* csr = chunk_allocator->convert_addr(vertices[vid]->get_cpos());
        for (degree_t d = 0; d < degree; ++d) {
            nebrs[d] = csr[d];
        }
        #ifdef MONITOR
        query_record.record_chunk(vertices[vid]->get_cpos() >> 32);
        #endif
        return degree;
    }

    void save_graph() {
        // save config
        std::ofstream ofs(SSDPATH + "/" + PREFIX + ".config");
        if (is_out_graph) {
            // nverts   nedges  max_level
            ofs << nverts << " " << nedges << " " << MAX_LEVEL << std::endl;
        }
        // level chunk size     chunk_number
        ofs << chunk_allocator->get_chunk_size() << " " << chunk_allocator->get_max_chunkID() << std::endl;
        ofs.close();
        // save vertex
        save_vertices();
        // save chunk and hub
        chunk_allocator->persist();
    }
};

class inplace_graph_t : public graph_t {
private:
    // chunk allocator
    chunk_allocator_csrchunk_t* chunk_allocator;
public:
    inplace_graph_t(vid_t nverts, index_t nedges, bool is_out_graph) : graph_t() {
        this->is_out_graph = is_out_graph;
        this->chunk_allocator = new chunk_allocator_csrchunk_t(is_out_graph);
    }

    ~inplace_graph_t() {
        delete chunk_allocator;
    }

    inline void init_chunk_allocator() {
        std::string pool_name = sblk_name;
        if (is_out_graph) pool_name += "_out_";
        else pool_name += "_in_";
        chunk_allocator->init(pool_name);
    }

    void convert_graph(index_t* csr_idx, vid_t* csr_adj) {
        #ifdef MONITOR
        degree_t nzvcount = 0;
        #endif

        #pragma omp parallel for num_threads(THD_COUNT) schedule (dynamic, 256*256)
        for (vid_t vid = 0; vid < nverts; ++vid) {
            degree_t deg = csr_idx[vid+1] - csr_idx[vid];
            if (deg) {
                #ifdef MONITOR
                ++nzvcount;
                #endif
                vertex_t* vert = vertices[vid];
                vert = new_vertex();
                set_vertex(vid, vert);

                vert->set_out_degree(deg);
                
                if (deg <= 2) {
                    for (degree_t d = 0; d < deg; ++d) {
                        vert->add_nebr(csr_adj[csr_idx[vid]+d]);
                    }
                } else {
                    // allocate vertex to a chunk
                    vid_t* adjlist = NULL;
                    cpos_t cpos = chunk_allocator->allocate(deg, &adjlist);
                    vert->set_cpos(cpos);
                    // copy adjlist to chunk
                    memcpy(adjlist, csr_adj+csr_idx[vid], deg*sizeof(vid_t));
                }
            }
            // if (vid == 12) {
            //     degree_t deg = get_out_degree(vid);
            //     std::cout << "test case" << std::endl;
            //     std::cout << "degree of " << vid << " is " << deg << std::endl;
            //     degree_t threshold = deg > 100 ? 100 : deg;
            //     std::cout << "The top " << threshold <<  " out neighbors of " << vid << " are: ";
            //     vid_t* nebrs = (vid_t*)calloc(sizeof(vid_t), deg);
            //     get_out_nebrs(vid, nebrs);
            //     for (degree_t i = 0; i < threshold; ++i) {
            //         std::cout << nebrs[i] << " ";
            //     }
            //     std::cout << std::endl;
            // }
        }
        #ifdef MONITOR
        max_chunkID = chunk_allocator->get_max_chunkID();
        std::cout << "max_chunkID = " << max_chunkID << std::endl;
        std::cout << "The number of non-zero degree vertices: " << nzvcount << std::endl;
        #endif
    }

    inline degree_t get_out_degree(vid_t vid) { 
        if (vertices[vid] == NULL) return 0;
        return vertices[vid]->get_out_degree(); 
    }

    inline degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { 
        if (vertices[vid] == NULL) return 0;
        degree_t degree = vertices[vid]->get_out_degree();
        if (degree == 0) return degree;
        if (degree <= 2) {
            vid_t* csr = vertices[vid]->get_nebrs();
            for (degree_t d = 0; d < degree; ++d) {
                nebrs[d] = csr[d];
            }
            #ifdef MONITOR
            query_record.record_inplace();
            #endif
        } else {
            vid_t* csr = chunk_allocator->convert_addr(vertices[vid]->get_cpos());
            for (degree_t d = 0; d < degree; ++d) {
                nebrs[d] = csr[d];
            }
            #ifdef MONITOR
            query_record.record_chunk(vertices[vid]->get_cpos() >> 32);
            #endif
        }
        return degree;
    }

    void save_graph() {
        // save config
        std::ofstream ofs(SSDPATH + "/" + PREFIX + ".config");
        if (is_out_graph) {
            // nverts   nedges  max_level
            ofs << nverts << " " << nedges << " " << MAX_LEVEL << std::endl;
        }
        // level chunk size     chunk_number
        ofs << chunk_allocator->get_chunk_size() << " " << chunk_allocator->get_max_chunkID() << std::endl;
        ofs.close();
        // save vertex
        save_vertices();
        // save chunk and hub
        chunk_allocator->persist();
    }
};

class regraph_t : public graph_t {
private:
    // chunk allocator
    chunk_allocator_csrchunk_t* chunk_allocator;
    vid_t* visited;
    vid_t* reorder_list;

public:
    regraph_t(vid_t nverts, index_t nedges, bool is_out_graph) {
        this->is_out_graph = is_out_graph;
        this->chunk_allocator = new chunk_allocator_csrchunk_t(is_out_graph);
    }

    ~regraph_t() {
        if (visited) free(visited);
        if (reorder_list) free(reorder_list);
        delete chunk_allocator;
    }

    inline void init_chunk_allocator() {
        std::string pool_name = sblk_name;
        if (is_out_graph) pool_name += "_out_";
        else pool_name += "_in_";
        chunk_allocator->init(pool_name);
    }

    inline vid_t re_order(index_t* csr_idx, vid_t* csr_adj, vid_t root = 12) {
        // run preprocess BFS
        reorder_list = (vid_t*)calloc(sizeof(vid_t), nverts);
        reorder_list[0] = root;
        visited[root] = 1;
        vid_t reorder_count = 1;
        vid_t reorder_list_tail = 1;
        for (vid_t i = 0; i < reorder_list_tail; ++i) {
            vid_t vid = reorder_list[i];
            degree_t degree = csr_idx[vid+1] - csr_idx[vid];
            vid_t* nebrs = csr_adj+csr_idx[vid];
            for (degree_t d = 0; d < degree; ++d) {
                vid_t nebr = nebrs[d];
                if (visited[nebr] == 0) {
                    visited[nebr] = 1;
                    reorder_list[reorder_list_tail++] = nebr;
                    reorder_count++;
                }
            }
        }
        #ifdef MONITOR
        std::cout << "The number of non-zero degree vertices: " << reorder_count << std::endl;
        std::cout << "reorder_list_tail = " << reorder_list_tail << std::endl;
        #endif
        return reorder_list_tail;
    }

    void convert_graph(index_t* csr_idx, vid_t* csr_adj) {
        // reorder
        visited = (vid_t*)calloc(sizeof(vid_t), nverts);
        reorder_list = (vid_t*)calloc(sizeof(vid_t), nverts);
        vid_t num_reorder = re_order(csr_idx, csr_adj, source);

        #ifdef MONITOR
        degree_t nzvcount = 0;
        #endif
        
        #pragma omp for schedule (dynamic, 256*256) nowait
        for (vid_t i = 0; i < num_reorder; ++i) {
            vid_t vid = reorder_list[i];
            degree_t deg = csr_idx[vid+1] - csr_idx[vid];
            if (deg) {
                #ifdef MONITOR
                ++nzvcount;
                #endif
                vertex_t* vert = vertices[vid];
                vert = new_vertex();
                set_vertex(vid, vert);

                vert->set_out_degree(deg);
                
                if (deg <= 2) {
                    for (degree_t d = 0; d < deg; ++d) {
                        vert->add_nebr(csr_adj[csr_idx[vid]+d]);
                    }
                } else {
                    // allocate vertex to a chunk
                    vid_t* adjlist = NULL;
                    cpos_t cpos = chunk_allocator->allocate(deg, &adjlist);
                    vert->set_cpos(cpos);
                    // copy adjlist to chunk
                    memcpy(adjlist, csr_adj+csr_idx[vid], deg*sizeof(vid_t));
                }
            }
        }

        for (vid_t vid = 0; vid < nverts; ++vid) {
            if (visited[vid]) continue;
            degree_t deg = csr_idx[vid+1] - csr_idx[vid];
            if (deg) {
                #ifdef MONITOR
                ++nzvcount;
                #endif
                vertex_t* vert = vertices[vid];
                vert = new_vertex();
                set_vertex(vid, vert);

                vert->set_out_degree(deg);
                
                if (deg <= 2) {
                    for (degree_t d = 0; d < deg; ++d) {
                        vert->add_nebr(csr_adj[csr_idx[vid]+d]);
                    }
                } else {
                    // allocate vertex to a chunk
                    vid_t* adjlist = NULL;
                    cpos_t cpos = chunk_allocator->allocate(deg, &adjlist);
                    vert->set_cpos(cpos);
                    // copy adjlist to chunk
                    memcpy(adjlist, csr_adj+csr_idx[vid], deg*sizeof(vid_t));
                }
            }
        }

        #ifdef MONITOR
        max_chunkID = chunk_allocator->get_max_chunkID();
        std::cout << "max_chunkID = " << max_chunkID << std::endl;
        std::cout << "The number of non-zero degree vertices: " << nzvcount << std::endl;
        #endif
                
        free(visited);
        free(reorder_list);
        visited = NULL;
        reorder_list = NULL;
    }

    inline degree_t get_out_degree(vid_t vid) { 
        if (vertices[vid] == NULL) return 0;
        return vertices[vid]->get_out_degree(); 
    }

    inline degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { 
        if (vertices[vid] == NULL) return 0;
        degree_t degree = vertices[vid]->get_out_degree();
        if (degree == 0) return degree;
        if (degree <= 2) {
            vid_t* csr = vertices[vid]->get_nebrs();
            for (degree_t d = 0; d < degree; ++d) {
                nebrs[d] = csr[d];
            }
            #ifdef MONITOR
            query_record.record_inplace();
            #endif
        } else {
            vid_t* csr = chunk_allocator->convert_addr(vertices[vid]->get_cpos());
            for (degree_t d = 0; d < degree; ++d) {
                nebrs[d] = csr[d];
            }
            #ifdef MONITOR
            query_record.record_chunk(vertices[vid]->get_cpos() >> 32);
            #endif
        }
        return degree;
    }

    void save_graph() {
        // save config
        std::ofstream ofs(SSDPATH + "/" + PREFIX + ".config");
        if (is_out_graph) {
            // nverts   nedges  max_level
            ofs << nverts << " " << nedges << " " << MAX_LEVEL - 1 << std::endl;
        }
        // level chunk size     chunk_number
        ofs << chunk_allocator->get_chunk_size() << " " << chunk_allocator->get_max_chunkID() + 1 << std::endl;
        ofs.close();
        // save vertex
        save_vertices();
        // save chunk and hub
        chunk_allocator->persist();
    }
};

class trigraph_t : public graph_t {
private:
    // chunk allocator
    chunk_allocator_trilevel_t* chunk_allocator;
    vid_t* visited;
    vid_t* reorder_list;

public:
    trigraph_t(vid_t nverts, index_t nedges, bool is_out_graph) {
        this->is_out_graph = is_out_graph;
        this->chunk_allocator = new chunk_allocator_trilevel_t(is_out_graph);
    }

    ~trigraph_t() {
        if (visited) free(visited);
        if (reorder_list) free(reorder_list);
        delete chunk_allocator;
    }

    inline void init_chunk_allocator() {
        std::string pool_name = sblk_name;
        if (is_out_graph) pool_name += "_out_";
        else pool_name += "_in_";
        chunk_allocator->init(pool_name);
    }

    inline vid_t re_order(index_t* csr_idx, vid_t* csr_adj, vid_t root = 12) {
        // run preprocess BFS
        reorder_list = (vid_t*)calloc(sizeof(vid_t), nverts);
        reorder_list[0] = root;
        visited[root] = 1;
        vid_t reorder_count = 1;
        vid_t reorder_list_tail = 1;
        for (vid_t i = 0; i < reorder_list_tail; ++i) {
            vid_t vid = reorder_list[i];
            degree_t degree = csr_idx[vid+1] - csr_idx[vid];
            vid_t* nebrs = csr_adj+csr_idx[vid];
            for (degree_t d = 0; d < degree; ++d) {
                vid_t nebr = nebrs[d];
                if (visited[nebr] == 0) {
                    visited[nebr] = 1;
                    reorder_list[reorder_list_tail++] = nebr;
                    reorder_count++;
                }
            }
        }
        #ifdef MONITOR
        std::cout << "The number of non-zero degree vertices: " << reorder_count << std::endl;
        std::cout << "reorder_list_tail = " << reorder_list_tail << std::endl;
        #endif
        return reorder_list_tail;
    }

    void convert_graph(index_t* csr_idx, vid_t* csr_adj) {
        // reorder
        visited = (vid_t*)calloc(sizeof(vid_t), nverts);
        reorder_list = (vid_t*)calloc(sizeof(vid_t), nverts);
        vid_t num_reorder = re_order(csr_idx, csr_adj, source);

        #ifdef MONITOR
        degree_t nzvcount = 0;
        #endif
        
        // #pragma omp for schedule (dynamic, 256*256) nowait
        for (vid_t i = 0; i < num_reorder; ++i) {
            vid_t vid = reorder_list[i];
            degree_t deg = csr_idx[vid+1] - csr_idx[vid];
            if (deg) {
                #ifdef MONITOR
                ++nzvcount;
                #endif
                vertex_t* vert = vertices[vid];
                vert = new_vertex();
                set_vertex(vid, vert);

                vert->set_out_degree(deg);
                
                if (deg <= 2) {
                    for (degree_t d = 0; d < deg; ++d) {
                        vert->add_nebr(csr_adj[csr_idx[vid]+d]);
                    }
                } else {
                    // allocate vertex to a chunk
                    vid_t* adjlist = NULL;
                    cpos_t cpos = chunk_allocator->allocate(deg, &adjlist);
                    vert->set_cpos(cpos);
                    // copy adjlist to chunk
                    memcpy(adjlist, csr_adj+csr_idx[vid], deg*sizeof(vid_t));
                }
            }
        }

        for (vid_t vid = 0; vid < nverts; ++vid) {
            if (visited[vid]) continue;
            degree_t deg = csr_idx[vid+1] - csr_idx[vid];
            if (deg) {
                #ifdef MONITOR
                ++nzvcount;
                #endif
                vertex_t* vert = vertices[vid];
                vert = new_vertex();
                set_vertex(vid, vert);

                vert->set_out_degree(deg);
                
                if (deg <= 2) {
                    for (degree_t d = 0; d < deg; ++d) {
                        vert->add_nebr(csr_adj[csr_idx[vid]+d]);
                    }
                } else {
                    // allocate vertex to a chunk
                    vid_t* adjlist = NULL;
                    cpos_t cpos = chunk_allocator->allocate(deg, &adjlist);
                    vert->set_cpos(cpos);
                    // copy adjlist to chunk
                    memcpy(adjlist, csr_adj+csr_idx[vid], deg*sizeof(vid_t));
                }
            }
        }

        // // test case
        // std::cout << "test case" << std::endl;
        // degree_t deg = get_out_degree(13757872);
        // std::cout << "degree of " << 13757872 << " is " << deg << std::endl;
        // cpos_t cpos = vertices[13757872]->get_cpos();
        // std::cout << "position of chunk = " << cpos << std::endl;
        // uint32_t cid = cpos >> 32;
        // std::cout << "cid = " << cid << std::endl;
        // uint32_t offset = cpos & 0xffffffff;
        // std::cout << "offset = " << offset << std::endl;


        #ifdef MONITOR
        max_chunkID = chunk_allocator->get_max_chunkID();
        max_offset = chunk_allocator->get_max_offset();
        std::cout << "max_chunkID = " << max_chunkID << std::endl;
        std::cout << "max_offset = " << max_offset / GB << "(GB)" << std::endl;
        std::cout << "The number of non-zero degree vertices: " << nzvcount << std::endl;

        #endif
                
        free(visited);
        free(reorder_list);
        visited = NULL;
        reorder_list = NULL;
    }

    void convert_without_reorder(index_t* csr_idx, vid_t* csr_adj) {
        #ifdef MONITOR
        degree_t nzvcount = 0;
        #endif
        
        for (vid_t vid = 0; vid < nverts; ++vid) {
            degree_t deg = csr_idx[vid+1] - csr_idx[vid];
            if (deg) {
                #ifdef MONITOR
                ++nzvcount;
                #endif
                vertex_t* vert = vertices[vid];
                vert = new_vertex();
                set_vertex(vid, vert);

                vert->set_out_degree(deg);
                
                if (deg <= 2) {
                    for (degree_t d = 0; d < deg; ++d) {
                        vert->add_nebr(csr_adj[csr_idx[vid]+d]);
                    }
                } else {
                    // allocate vertex to a chunk
                    vid_t* adjlist = NULL;
                    cpos_t cpos = chunk_allocator->allocate(deg, &adjlist);
                    vert->set_cpos(cpos);
                    // copy adjlist to chunk
                    memcpy(adjlist, csr_adj+csr_idx[vid], deg*sizeof(vid_t));
                }
            }
        }
    }

    inline degree_t get_out_degree(vid_t vid) { 
        if (vertices[vid] == NULL) return 0;
        return vertices[vid]->get_out_degree(); 
    }

    inline degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { 
        if (vertices[vid] == NULL) return 0;
        degree_t degree = vertices[vid]->get_out_degree();
        if (degree == 0) return degree;
        if (degree <= 2) {
            vid_t* csr = vertices[vid]->get_nebrs();
            for (degree_t d = 0; d < degree; ++d) {
                nebrs[d] = csr[d];
            }
            #ifdef MONITOR
            query_record.record_inplace();
            #endif
        } else {
            vid_t* csr = chunk_allocator->convert_addr(vertices[vid]->get_cpos(), degree);
            for (degree_t d = 0; d < degree; ++d) {
                nebrs[d] = csr[d];
            }
            #ifdef MONITOR
            query_record.record_chunk(vertices[vid]->get_cpos() >> 32);
            #endif
        }
        return degree;
    }

    void save_graph() {
        // save config
        std::ofstream ofs(SSDPATH + "/" + PREFIX + ".config", std::ofstream::out | std::ofstream::app);
        if (is_out_graph) {
            // nverts   nedges  max_chunk_level
            ofs << nverts << " " << nedges << " " << MAX_LEVEL - 1 << std::endl;
        }
        // super block size
        ofs << chunk_allocator->get_max_offset() << std::endl;
        //      level max degree     level chunk size     chunk_number
        ofs << chunk_allocator->get_threshold() << " " << chunk_allocator->get_chunk_size() << " " << chunk_allocator->get_max_chunkID() + 1 << std::endl;
        ofs.close();
        // save vertex
        save_vertices();
        // save chunk and hub
        chunk_allocator->persist();
    }

};

class reordergraph_t : public graph_t {
private:
    // chunk allocator
    chunk_allocator_trilevel_t* chunk_allocator;
    vid_t* visited;
    vid_t* reorder_list;

public:
    reordergraph_t(vid_t nverts, index_t nedges, bool is_out_graph) {
        this->is_out_graph = is_out_graph;
        this->chunk_allocator = new chunk_allocator_trilevel_t(is_out_graph);
    }

    ~reordergraph_t() {
        if (visited) free(visited);
        if (reorder_list) free(reorder_list);
        delete chunk_allocator;
    }

    inline void init_chunk_allocator() {
        std::string pool_name = sblk_name;
        if (is_out_graph) pool_name += "_out_";
        else pool_name += "_in_";
        chunk_allocator->init(pool_name);
    }

    inline vid_t re_order(index_t* csr_idx, vid_t* csr_adj, vid_t root = 12) {
        // run preprocess BFS
        reorder_list[0] = root;
        visited[root] = 1;
        vid_t reorder_list_tail = 1;

        for (vid_t i = 0; i < reorder_list_tail; ++i) {
            vid_t vid = reorder_list[i];
            degree_t degree = csr_idx[vid+1] - csr_idx[vid];
            vid_t* nebrs = csr_adj+csr_idx[vid];
            for (degree_t d = 0; d < degree; ++d) {
                vid_t nebr = nebrs[d];
                if (visited[nebr] == 0) {
                    visited[nebr] = 1;
                    reorder_list[reorder_list_tail++] = nebr;
                }
            }
        }
        #ifdef MONITOR
        std::cout << "reorder_list_tail = " << reorder_list_tail << std::endl;
        #endif
        return reorder_list_tail;
    }

    void convert_graph(index_t* csr_idx, vid_t* csr_adj) {
        // reorder
        visited = (vid_t*)calloc(sizeof(vid_t), nverts);
        reorder_list = (vid_t*)calloc(sizeof(vid_t), nverts);
        vid_t num_reorder = re_order(csr_idx, csr_adj, source);

        #ifdef MONITOR
        degree_t nzvcount = 0;
        #endif
        
        // #pragma omp for schedule (dynamic, 256*256) nowait
        for (vid_t i = 0; i < num_reorder; ++i) {
            vid_t vid = reorder_list[i];
            degree_t deg = csr_idx[vid+1] - csr_idx[vid];
            if (deg) {
                #ifdef MONITOR
                ++nzvcount;
                #endif
                vertex_t* vert = vertices[vid];
                vert = new_vertex();
                set_vertex(vid, vert);

                vert->set_out_degree(deg);
                
                if (deg <= 2) {
                    for (degree_t d = 0; d < deg; ++d) {
                        vert->add_nebr(csr_adj[csr_idx[vid]+d]);
                    }
                } else {
                    // allocate vertex to a chunk
                    vid_t* adjlist = NULL;
                    cpos_t cpos = chunk_allocator->allocate(deg, &adjlist);
                    vert->set_cpos(cpos);
                    // copy adjlist to chunk
                    memcpy(adjlist, csr_adj+csr_idx[vid], deg*sizeof(vid_t));
                }
            }
        }

        vid_t total_num_reorder = num_reorder;

        for (vid_t vid = 0; vid < nverts; ++vid) {
            if (visited[vid]) continue;
            degree_t deg = csr_idx[vid+1] - csr_idx[vid];
            if (deg) {
                #ifdef MONITOR
                ++nzvcount;
                #endif
                vertex_t* vert = vertices[vid];
                vert = new_vertex();
                set_vertex(vid, vert);

                vert->set_out_degree(deg);
                
                if (deg <= 2) {
                    for (degree_t d = 0; d < deg; ++d) {
                        vert->add_nebr(csr_adj[csr_idx[vid]+d]);
                    }
                } else {
                    // allocate vertex to a chunk
                    vid_t* adjlist = NULL;
                    cpos_t cpos = chunk_allocator->allocate(deg, &adjlist);
                    vert->set_cpos(cpos);
                    // copy adjlist to chunk
                    memcpy(adjlist, csr_adj+csr_idx[vid], deg*sizeof(vid_t));
                }
            }
            reorder_list[total_num_reorder++] = vid;
        }

        #ifdef MONITOR
        max_chunkID = chunk_allocator->get_max_chunkID();
        max_offset = chunk_allocator->get_max_offset();
        std::cout << "max_chunkID = " << max_chunkID << std::endl;
        std::cout << "max_offset = " << max_offset / GB << "(GB)" << std::endl;
        std::cout << "The number of non-zero degree vertices: " << nzvcount << std::endl;
        std::cout << "total_num_reorder = " << total_num_reorder << std::endl;
        for (vid_t i = 0; i < 5; ++i) {
            std::cout << reorder_list[i] << " ";
        }
        std::cout << std::endl;
        #endif
                
        free(visited);
        visited = NULL;
    }

    inline degree_t get_out_degree(vid_t vid) { 
        if (vertices[vid] == NULL) return 0;
        return vertices[vid]->get_out_degree(); 
    }

    inline degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { 
        if (vertices[vid] == NULL) return 0;
        degree_t degree = vertices[vid]->get_out_degree();
        if (degree == 0) return degree;
        if (degree <= 2) {
            vid_t* csr = vertices[vid]->get_nebrs();
            for (degree_t d = 0; d < degree; ++d) {
                nebrs[d] = csr[d];
            }
            #ifdef MONITOR
            query_record.record_inplace();
            #endif
        } else {
            vid_t* csr = chunk_allocator->convert_addr(vertices[vid]->get_cpos(), degree);
            for (degree_t d = 0; d < degree; ++d) {
                nebrs[d] = csr[d];
            }
            #ifdef MONITOR
            query_record.record_chunk(vertices[vid]->get_cpos() >> 32);
            #endif
        }
        return degree;
    }

    void save_reorder_list() {
        std::string reorder_list_file = SSDPATH + "/" + PREFIX + ".reorder";
        size_t size = nverts * sizeof(vid_t) * 2;
        int fd = open(reorder_list_file.c_str(), O_RDWR | O_CREAT, 00777);
        if (fd == -1) {
            std::cout << "open file error" << std::endl;
            exit(1);
        }
        if (ftruncate(fd, size) == -1) {
            std::cout << "ftruncate error" << std::endl;
            exit(1);
        }
        vid_t* addr = (vid_t*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (addr == MAP_FAILED) {
            std::cout << "mmap error" << std::endl;
            exit(1);
        }

        if (is_out_graph) {
            memcpy(addr, reorder_list, nverts * sizeof(vid_t));
            // debug. print out the first hundred elements
            std::cout << "reorder_list for out graph: ";
            for (vid_t i = 0; i < 100; ++i) {
                std::cout << addr[i] << " ";
            }
            std::cout << std::endl;
        } else {
            memcpy(addr + nverts, reorder_list, nverts * sizeof(vid_t));
            // debug. print out the first hundred elements
            std::cout << "reorder_list for in graph: ";
            for (vid_t i = 0; i < 100; ++i) {
                std::cout << addr[i+nverts] << " ";
            }
            std::cout << std::endl;
        }
        msync(addr, size, MS_SYNC);
        munmap(addr, size);
        close(fd);
    }

    void save_graph() {
        // save config
        std::ofstream ofs(SSDPATH + "/" + PREFIX + ".config", std::ofstream::out | std::ofstream::app);
        if (is_out_graph) {
            // nverts   nedges  max_chunk_level
            ofs << nverts << " " << nedges << " " << MAX_LEVEL - 1 << std::endl;
        }
        // super block size
        ofs << chunk_allocator->get_max_offset() << std::endl;
        //      level max degree     level chunk size     chunk_number
        ofs << chunk_allocator->get_threshold() << " " << chunk_allocator->get_chunk_size() << " " << chunk_allocator->get_max_chunkID() + 1 << std::endl;
        ofs.close();
        // save vertex
        save_vertices();
        // save chunk and hub
        chunk_allocator->persist();
        // save reorder list using mmap
        save_reorder_list();
    }
};