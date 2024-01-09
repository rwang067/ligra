#pragma once
#include "graph.hpp"
#include "args_config.hpp"

class ChunkGraph {
public:
    virtual ~ChunkGraph() {}
    virtual vid_t get_vcount() = 0;
    virtual index_t get_ecount() = 0;
    virtual degree_t get_out_degree(vid_t vid) = 0;
    virtual degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) = 0;
    virtual degree_t get_in_degree(vid_t vid) = 0;
    virtual degree_t get_in_nebrs(vid_t vid, vid_t* nebrs) = 0;

    vid_t get_source_vertex() { return source; }
    uint32_t get_query_count() { return QUERY; }
};


class CSRGraph : public ChunkGraph {
private:
    csrgraph_t* out_graph;
    csrgraph_t* in_graph;
    
public:
    CSRGraph() {
        this->out_graph = new csrgraph_t(nverts, nedges, true);
        this->in_graph = new csrgraph_t(nverts, nedges, false);
    }
    ~CSRGraph() {
        delete out_graph;
        delete in_graph;
    }

    void import_graph() {
        double start = mywtime();
        out_graph->import_csr();
        in_graph->import_csr();
        double end = mywtime();
        ofs << "import graph time = " << end - start << std::endl;
    }

    void count_degree() {
        out_graph->count_degree();
        in_graph->count_degree();
        out_graph->print_degree();
        in_graph->print_degree();
    }

    void convert_blaze() {
        double start = mywtime();
        out_graph->convert_blaze();
        in_graph->convert_blaze();
        double end = mywtime();
        ofs << "convert blaze graph time = " << end - start << std::endl;
        std::cout << "convert blaze graph time = " << end - start << std::endl;
    }

    vid_t get_vcount() { return out_graph->get_vcount(); }
    index_t get_ecount() { return out_graph->get_ecount(); }

    degree_t get_out_degree(vid_t vid) { return out_graph->get_out_degree(vid); }
    degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { return out_graph->get_out_nebrs(vid, nebrs); }
    degree_t get_in_degree(vid_t vid) { return in_graph->get_out_degree(vid); }
    degree_t get_in_nebrs(vid_t vid, vid_t* nebrs) { return in_graph->get_out_nebrs(vid, nebrs); }
};

class ConvertGraph : public ChunkGraph {
protected:
    // Load graph data
    index_t* csr_idx, *csr_idx_in;
    vid_t* csr_adj, *csr_adj_in;
    size_t size_idx = 0, size_idx_in = 0;
    size_t size_adj = 0, size_adj_in = 0;
    
public:
    ~ConvertGraph() {
        if (csr_idx) free(csr_idx);
        if (csr_idx_in) free(csr_idx_in);
        if (csr_adj) free(csr_adj);
        if (csr_adj_in) free(csr_adj_in);
    }

    void load_csr(bool is_out_graph = true) {
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
            size_idx_in = alloc_and_read_file(filepath + "/" + PREFIX + ".ridx", &buf_idx, (nverts+1)*sizeof(index_t));
            size_adj_in = alloc_and_read_file(filepath + "/" + PREFIX + ".radj", &buf_adj);

            csr_idx_in = (index_t*)buf_idx;
            csr_adj_in = (vid_t*)buf_adj;
            csr_idx_in[nverts] = nedges;
        }
    }

    void free_csr(bool is_out_graph = true) {
        char* buf_idx, *buf_adj;
        if (is_out_graph) {
            buf_idx = (char*)csr_idx;
            buf_adj = (char*)csr_adj;
            free_local_buf(size_idx, &buf_idx, "idx");
            free_local_buf(size_adj, &buf_adj, "adj");
            csr_adj = NULL;
            csr_idx = NULL;
            size_idx = size_adj = 0;
        } else {
            buf_idx = (char*)csr_idx_in;
            buf_adj = (char*)csr_adj_in;
            free_local_buf(size_idx_in, &buf_idx, "ridx");
            free_local_buf(size_adj_in, &buf_adj, "radj");
            csr_adj_in = NULL;
            csr_idx_in = NULL;
            size_idx_in = size_adj_in = 0;
        }
    }

    virtual void convert_graph() = 0;
    virtual vid_t get_vcount() = 0;
    virtual index_t get_ecount() = 0;
    virtual degree_t get_out_degree(vid_t vid) = 0;
    virtual degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) = 0;
    virtual degree_t get_in_degree(vid_t vid) = 0;
    virtual degree_t get_in_nebrs(vid_t vid, vid_t* nebrs) = 0;
};

class BaseChunkGraph : public ConvertGraph {
public:
    BaseChunkGraph() {
        this->out_graph = new csrchunk_graph_t(nverts, nedges, true);
        this->in_graph = new csrchunk_graph_t(nverts, nedges, false);
    }
    ~BaseChunkGraph() {
        delete out_graph;
        delete in_graph;
    }

    void convert_graph() {
        double start = mywtime();
        load_csr(true);
        out_graph->init_chunk_allocator();
        out_graph->convert_graph(csr_idx, csr_adj);
        free_csr(true);

        load_csr(false);
        in_graph->init_chunk_allocator();
        in_graph->convert_graph(csr_idx_in, csr_adj_in);
        free_csr(false);
        double end = mywtime();
        ofs << "convert graph time = " << end - start << std::endl;
    }

    void save_graph() {
        out_graph->save_graph();
        in_graph->save_graph();
    }

    vid_t get_vcount() { return out_graph->get_vcount(); }
    index_t get_ecount() { return out_graph->get_ecount(); }
    degree_t get_out_degree(vid_t vid) { return out_graph->get_out_degree(vid); }
    degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { return out_graph->get_out_nebrs(vid, nebrs); }
    degree_t get_in_degree(vid_t vid) { return in_graph->get_out_degree(vid); }
    degree_t get_in_nebrs(vid_t vid, vid_t* nebrs) { return in_graph->get_out_nebrs(vid, nebrs); }
private:
    csrchunk_graph_t* out_graph;
    csrchunk_graph_t* in_graph;
};

class MiniVertexGraph : public ChunkGraph {
private:
    minivertex_graph_t* out_graph;
    minivertex_graph_t* in_graph;
public:
    MiniVertexGraph() {
        this->out_graph = new minivertex_graph_t(nverts, nedges, true);
        this->in_graph = new minivertex_graph_t(nverts, nedges, false);
    }
    ~MiniVertexGraph() {
        delete out_graph;
        delete in_graph;
    }

    void convert_graph() {
        double start = mywtime();
        out_graph->convert_graph();
        in_graph->convert_graph();
        double end = mywtime();
        ofs << "convert mini graph time = " << end - start << std::endl;
        std::cout << "convert mini graph time = " << end - start << std::endl;
    }

    void convert_graph2() {
        double start = mywtime();
        out_graph->convert_graph2();
        in_graph->convert_graph2();
        double end = mywtime();
        ofs << "convert mini graph time = " << end - start << std::endl;
        std::cout << "convert mini graph time = " << end - start << std::endl;
    }

    vid_t get_vcount() { return out_graph->get_vcount(); }
    index_t get_ecount() { return out_graph->get_ecount(); }
    degree_t get_out_degree(vid_t vid) { return out_graph->get_out_degree(vid); }
    degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { return out_graph->get_out_nebrs(vid, nebrs); }
    degree_t get_in_degree(vid_t vid) { return in_graph->get_out_degree(vid); }
    degree_t get_in_nebrs(vid_t vid, vid_t* nebrs) { return in_graph->get_out_nebrs(vid, nebrs); }
};


class InplaceGraph : public ConvertGraph {

public:
    InplaceGraph() {
        this->out_graph = new inplace_graph_t(nverts, nedges, true);
        this->in_graph = new inplace_graph_t(nverts, nedges, false);
    }
    ~InplaceGraph() {
        delete out_graph;
        delete in_graph;
    }

    void convert_graph() {
        double start = mywtime();
        load_csr(true);
        out_graph->init_chunk_allocator();
        out_graph->convert_graph(csr_idx, csr_adj);
        free_csr(true);

        load_csr(false);
        in_graph->init_chunk_allocator();
        in_graph->convert_graph(csr_idx_in, csr_adj_in);
        free_csr(false);
        double end = mywtime();
        ofs << "convert graph time = " << end - start << std::endl;
    }

    void save_graph() {
        out_graph->save_graph();
        in_graph->save_graph();
    }

    vid_t get_vcount() { return out_graph->get_vcount(); }
    index_t get_ecount() { return out_graph->get_ecount(); }
    degree_t get_out_degree(vid_t vid) { return out_graph->get_out_degree(vid); }
    degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { return out_graph->get_out_nebrs(vid, nebrs); }
    degree_t get_in_degree(vid_t vid) { return in_graph->get_out_degree(vid); }
    degree_t get_in_nebrs(vid_t vid, vid_t* nebrs) { return in_graph->get_out_nebrs(vid, nebrs); }
private:
    inplace_graph_t* out_graph;
    inplace_graph_t* in_graph;
};

class ReGraph : public ConvertGraph {
public:
    ReGraph() {
        this->out_graph = new regraph_t(nverts, nedges, true);
        this->in_graph = new regraph_t(nverts, nedges, false);
    }
    ~ReGraph() {
        delete out_graph;
        delete in_graph;
    }

    void convert_graph() {
        double start = mywtime();
        load_csr(true);
        out_graph->init_chunk_allocator();
        out_graph->convert_graph(csr_idx, csr_adj);
        free_csr(true);

        load_csr(false);
        in_graph->init_chunk_allocator();
        in_graph->convert_graph(csr_idx_in, csr_adj_in);
        free_csr(false);
        double end = mywtime();
        ofs << "convert graph time = " << end - start << std::endl;
    }

    void save_graph() {
        out_graph->save_graph();
        in_graph->save_graph();
    }

    vid_t get_vcount() { return out_graph->get_vcount(); }
    index_t get_ecount() { return out_graph->get_ecount(); }
    degree_t get_out_degree(vid_t vid) { return out_graph->get_out_degree(vid); }
    degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { return out_graph->get_out_nebrs(vid, nebrs); }
    degree_t get_in_degree(vid_t vid) { return in_graph->get_out_degree(vid); }
    degree_t get_in_nebrs(vid_t vid, vid_t* nebrs) { return in_graph->get_out_nebrs(vid, nebrs); }
private:
    regraph_t* out_graph;
    regraph_t* in_graph;
};

class TriLevelGraph : public ConvertGraph {
public:
    TriLevelGraph() {
        this->out_graph = new trigraph_t(nverts, nedges, true);
        this->in_graph = new trigraph_t(nverts, nedges, false);
    }
    ~TriLevelGraph() {
        delete out_graph;
        delete in_graph;
    }

    void convert_graph_without_reorder() {
        double start = mywtime();
        load_csr(true);
        out_graph->init_chunk_allocator();
        out_graph->convert_without_reorder(csr_idx, csr_adj);
        free_csr(true);

        load_csr(false);
        in_graph->init_chunk_allocator();
        in_graph->convert_without_reorder(csr_idx_in, csr_adj_in);
        free_csr(false);
        double end = mywtime();
        ofs << "convert graph time = " << end - start << std::endl;
    }

    void convert_graph() {
        double start = mywtime();
        load_csr(true);
        out_graph->init_chunk_allocator();
        out_graph->convert_graph(csr_idx, csr_adj);
        free_csr(true);

        load_csr(false);
        in_graph->init_chunk_allocator();
        in_graph->convert_graph(csr_idx_in, csr_adj_in);
        free_csr(false);
        double end = mywtime();
        ofs << "convert graph time = " << end - start << std::endl;
    }

    void save_graph() {
        out_graph->save_graph();
        in_graph->save_graph();
    }

    vid_t get_vcount() { return out_graph->get_vcount(); }
    index_t get_ecount() { return out_graph->get_ecount(); }
    degree_t get_out_degree(vid_t vid) { return out_graph->get_out_degree(vid); }
    degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { return out_graph->get_out_nebrs(vid, nebrs); }
    degree_t get_in_degree(vid_t vid) { return in_graph->get_out_degree(vid); }
    degree_t get_in_nebrs(vid_t vid, vid_t* nebrs) { return in_graph->get_out_nebrs(vid, nebrs); }
private:
    trigraph_t* out_graph;
    trigraph_t* in_graph;
};

class ReorderGraph : public ConvertGraph {
public:
    ReorderGraph() {
        this->out_graph = new reordergraph_t(nverts, nedges, true);
        this->in_graph = new reordergraph_t(nverts, nedges, false);
    }
    ~ReorderGraph() {
        delete out_graph;
        delete in_graph;
    }

    void convert_graph() {
        double start = mywtime();
        load_csr(true);
        load_csr(false);
        out_graph->init_chunk_allocator();
        out_graph->convert_graph(csr_idx, csr_adj, csr_idx_in);
        
        in_graph->init_chunk_allocator();
        in_graph->convert_graph(csr_idx_in, csr_adj_in, csr_idx);
        free_csr(true);
        free_csr(false);
        double end = mywtime();
        ofs << "convert graph time = " << end - start << std::endl;
        std::cout << "convert graph time = " << end - start << std::endl;
    }

    void save_graph() {
        out_graph->save_graph();
        in_graph->save_graph();
    }

    vid_t get_vcount() { return out_graph->get_vcount(); }
    index_t get_ecount() { return out_graph->get_ecount(); }
    degree_t get_out_degree(vid_t vid) { return out_graph->get_out_degree(vid); }
    degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { return out_graph->get_out_nebrs(vid, nebrs); }
    degree_t get_in_degree(vid_t vid) { return in_graph->get_out_degree(vid); }
    degree_t get_in_nebrs(vid_t vid, vid_t* nebrs) { return in_graph->get_out_nebrs(vid, nebrs); }
private:
    reordergraph_t* out_graph;
    reordergraph_t* in_graph;
};

class ReorderIDGraph : public ConvertGraph {
public:
    ReorderIDGraph() {
        this->out_graph = new reorderidgraph_t(nverts, nedges, true);
        this->in_graph = new reorderidgraph_t(nverts, nedges, false);
    }
    ~ReorderIDGraph() {
        delete out_graph;
        delete in_graph;
    }

    void convert_graph() {
        double start = mywtime();
        load_csr(true);
        load_csr(false);
        out_graph->init_chunk_allocator();
        in_graph->init_chunk_allocator();

        vid_t* reorder_list = (vid_t*)calloc(nverts, sizeof(vid_t));    // save the reorder result
        vid_t* origin2reorder = (vid_t*)calloc(nverts, sizeof(vid_t));  // save mapping from original id to new id

        out_graph->init_reorder_list(reorder_list, origin2reorder);
        in_graph->init_reorder_list(reorder_list, origin2reorder);
        out_graph->convert_graph(csr_idx, csr_adj, csr_idx_in);
        in_graph->convert_id(csr_idx_in, csr_adj_in);
        
        free_csr(true);
        free_csr(false);
        double end = mywtime();
        ofs << "convert graph time = " << end - start << std::endl;
        std::cout << "convert graph time = " << end - start << std::endl;

        // test
        if (source != -1) {
            std::cout << "source = " << source << ", reorder source = " << origin2reorder[source] << std::endl;
        }
        vid_t origin_list[5] = {12, 300, 1024, 65535, 1023512};
        for (int i = 0; i < 5; i++) {
            vid_t origin_id = origin_list[i];
            vid_t new_id = origin2reorder[origin_id];
            std::cout << "origin id = " << origin_id << ", reorder id = " << new_id << std::endl;
            degree_t out_degree = out_graph->get_out_degree(new_id);
            degree_t in_degree = in_graph->get_out_degree(new_id);
            std::cout << "out degree = " << out_degree << ", in degree = " << in_degree << std::endl;
            vid_t* out_nebrs = (vid_t*)malloc(out_degree * sizeof(vid_t));
            out_graph->get_out_nebrs(new_id, out_nebrs);
            degree_t threshold = out_degree > 100 ? 100 : out_degree;
            std::cout << "out nebrs: ";
            for (degree_t j = 0; j < threshold; j++) {
                std::cout << out_nebrs[j] << " ";
            }
            std::cout << std::endl;
            std::cout << "original out nebrs: ";
            for (degree_t j = 0; j < threshold; j++) {
                std::cout << reorder_list[out_nebrs[j]] << " ";
            }
            std::cout << std::endl;
            free(out_nebrs);
            vid_t* in_nebrs = (vid_t*)malloc(in_degree * sizeof(vid_t));
            in_graph->get_out_nebrs(new_id, in_nebrs);
            threshold = in_degree > 100 ? 100 : in_degree;
            std::cout << "in nebrs: ";
            for (degree_t j = 0; j < threshold; j++) {
                std::cout << in_nebrs[j] << " ";
            }
            std::cout << std::endl;
            std::cout << "original in nebrs: ";
            for (degree_t j = 0; j < threshold; j++) {
                std::cout << reorder_list[in_nebrs[j]] << " ";
            }
            std::cout << std::endl;
            free(in_nebrs);
        }

        free(reorder_list);
        free(origin2reorder);
    }

    void convert_graph_in() {
        double start = mywtime();
        load_csr(true);
        load_csr(false);
        out_graph->init_chunk_allocator();
        in_graph->init_chunk_allocator();

        vid_t* reorder_list = (vid_t*)calloc(nverts, sizeof(vid_t));    // save the reorder result
        vid_t* origin2reorder = (vid_t*)calloc(nverts, sizeof(vid_t));  // save mapping from original id to new id

        out_graph->init_reorder_list(reorder_list, origin2reorder);
        in_graph->init_reorder_list(reorder_list, origin2reorder);

        in_graph->convert_graph(csr_idx_in, csr_adj_in, csr_idx);
        out_graph->convert_id(csr_idx, csr_adj);
        
        free_csr(true);
        free_csr(false);
        double end = mywtime();
        ofs << "convert graph time = " << end - start << std::endl;
        std::cout << "convert graph time = " << end - start << std::endl;

        // test
        if (source != -1) {
            std::cout << "source = " << source << ", reorder source = " << origin2reorder[source] << std::endl;
        }
        vid_t origin_list[5] = {12, 300, 1024, 65535, 1023512};
        for (int i = 0; i < 5; i++) {
            vid_t origin_id = origin_list[i];
            vid_t new_id = origin2reorder[origin_id];
            std::cout << "origin id = " << origin_id << ", reorder id = " << new_id << std::endl;
            degree_t out_degree = out_graph->get_out_degree(new_id);
            degree_t in_degree = in_graph->get_out_degree(new_id);
            std::cout << "out degree = " << out_degree << ", in degree = " << in_degree << std::endl;
            vid_t* out_nebrs = (vid_t*)malloc(out_degree * sizeof(vid_t));
            out_graph->get_out_nebrs(new_id, out_nebrs);
            degree_t threshold = out_degree > 100 ? 100 : out_degree;
            std::cout << "out nebrs: ";
            for (degree_t j = 0; j < threshold; j++) {
                std::cout << out_nebrs[j] << " ";
            }
            std::cout << std::endl;
            std::cout << "original out nebrs: ";
            for (degree_t j = 0; j < threshold; j++) {
                std::cout << reorder_list[out_nebrs[j]] << " ";
            }
            std::cout << std::endl;
            free(out_nebrs);
            vid_t* in_nebrs = (vid_t*)malloc(in_degree * sizeof(vid_t));
            in_graph->get_out_nebrs(new_id, in_nebrs);
            threshold = in_degree > 100 ? 100 : in_degree;
            std::cout << "in nebrs: ";
            for (degree_t j = 0; j < threshold; j++) {
                std::cout << in_nebrs[j] << " ";
            }
            std::cout << std::endl;
            std::cout << "original in nebrs: ";
            for (degree_t j = 0; j < threshold; j++) {
                std::cout << reorder_list[in_nebrs[j]] << " ";
            }
            std::cout << std::endl;
            free(in_nebrs);
        }

        free(reorder_list);
        free(origin2reorder);
    }

    void save_graph() {
        out_graph->save_graph();
        in_graph->save_graph();
    }

    vid_t get_vcount() { return out_graph->get_vcount(); }
    index_t get_ecount() { return out_graph->get_ecount(); }
    degree_t get_out_degree(vid_t vid) { return out_graph->get_out_degree(vid); }
    degree_t get_out_nebrs(vid_t vid, vid_t* nebrs) { return out_graph->get_out_nebrs(vid, nebrs); }
    degree_t get_in_degree(vid_t vid) { return in_graph->get_out_degree(vid); }
    degree_t get_in_nebrs(vid_t vid, vid_t* nebrs) { return in_graph->get_out_nebrs(vid, nebrs); }
private:
    reorderidgraph_t* out_graph;
    reorderidgraph_t* in_graph;
};