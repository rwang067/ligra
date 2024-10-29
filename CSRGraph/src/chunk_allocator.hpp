#pragma once
#include "store.hpp"
#include "mempool.hpp"
#include "common.hpp"
#include "mem_bulk.hpp"


class chunk_allocator_t {
protected:
    mempool_t** sblk_pools;
    bool is_out_graph;
public:
    chunk_allocator_t() {
        sblk_pools = NULL;
    }

    void init(std::string pool_name) {
        sblk_pools = (mempool_t**)calloc(sizeof(mempool_t*), MAX_LEVEL);

        std::string level_names[MAX_LEVEL];
        for (uint32_t i = 0; i < MAX_LEVEL; ++i) level_names[i] = "level" + std::to_string(i);

        for (uint32_t level = 0; level < MAX_LEVEL; ++level) {
            size_t size = SBLK_POOL_SIZE / 2 / MAX_LEVEL;
            char* start;
            // alloc for super-vertex
            std::string file = SSDPATH + pool_name + level_names[level];
            start = (char*)ssd_alloc(file.c_str(), size);
            logstream(LOG_DEBUG) << "sblk_pools[" << level << "] allocated " << size/GB << "GB on SSD, start = " << (void*)start << std::endl;

            sblk_pools[level] = new mempool_t();
            char* pool_name = new char [sizeof(level_names[level])];
            strcpy(pool_name, (sblk_name + level_names[level]).c_str());
            sblk_pools[level]->init(start, size, ALIGN_SIZE, (const char*)pool_name);
        }
    }
}; 

class chunk_allocator_csrchunk_t : public chunk_allocator_t {
public:
    chunk_allocator_csrchunk_t(bool _is_out_graph) : chunk_allocator_t() {
        is_out_graph = _is_out_graph;
    }

    void init(std::string pool_name) {
        chunk_allocator_t::init(pool_name);
        chunks = new csrchunk_t(sblk_pools);
    }

    cpos_t allocate(degree_t degree, vid_t** nebrs) {
        size_t vsize = degree * sizeof(vid_t);
        cpos_t cpos = 0;
        if (vsize > chunks->get_chunk_size()) {
            cpos = chunks->allocate_super(vsize, nebrs);
        } else {
            cpos = chunks->allocate_medium(vsize, nebrs);
        }
        return cpos;
    }

    void persist() {
        std::string pool_name = sblk_name + (is_out_graph ? "_out_" : "_in_");
        // persist chunk vertices
        std::string level_names[MAX_LEVEL];
        for (uint32_t i = 0; i < MAX_LEVEL; ++i) level_names[i] = "level" + std::to_string(i);
        size_t resize[MAX_LEVEL];
        resize[0] = (chunks->get_max_chunkID() + 1) * chunks->get_chunk_size();

        std::string SUFFIX[MAX_LEVEL];
        if (is_out_graph) {
            SUFFIX[0] = {".adj.chunk0"}; 
        } else {
            SUFFIX[0] = {".radj.chunk0"};
        }

        for (uint32_t level = 0; level < MAX_LEVEL; ++level) {        
            msync(sblk_pools[level]->get_base(), SBLK_POOL_SIZE / 2 / MAX_LEVEL, MS_SYNC);
            std::string filepath = SSDPATH + pool_name + level_names[level];
            int fd = open(filepath.c_str(), O_RDWR|O_CREAT, 00777);
            if (ftruncate(fd, resize[level]) == -1) {
                std::cout << "Could not ftruncate. error: " << strerror(errno) << std::endl;
                close(fd);
                exit(1);
            }
            std::string newname = SSDPATH + "/" + PREFIX + SUFFIX[level];
            if (rename(filepath.c_str(), newname.c_str())) {
                std::cout << "Could not rename. error: " << strerror(errno) << std::endl;
                exit(1);
            }
        }
    }

    vid_t* convert_addr(cpos_t cpos) {
        char* base = sblk_pools[0]->get_base();
        cpos_t chunk_id = cpos >> 32;
        cpos_t offset = cpos & 0xffffffff;
        return (vid_t*)(base + chunk_id * chunks->get_chunk_size() + offset);
    }

    cpos_t get_max_chunkID() {
        return chunks->get_max_chunkID();
    }

    size_t get_chunk_size() {
        return chunks->get_chunk_size();
    }

private:
    csrchunk_t* chunks;
};

class chunk_allocator_trilevel_t : public chunk_allocator_t {
public:
    chunk_allocator_trilevel_t(bool _is_out_graph) : chunk_allocator_t() {
        is_out_graph = _is_out_graph;
    }

    void init(std::string pool_name) {
        chunk_allocator_t::init(pool_name);
        chunks = new trilevel_t(sblk_pools);
    }

    cpos_t allocate(degree_t degree, vid_t** nebrs) {
        cpos_t cpos = 0;
        if (degree > threshold) {
            cpos = chunks->allocate_super(degree * sizeof(vid_t), nebrs);
        } else {
            cpos = chunks->allocate_medium(degree * sizeof(vid_t), nebrs);
        }
        return cpos;
    }

    void persist() {
        std::string pool_name = sblk_name + (is_out_graph ? "_out_" : "_in_");
        // persist chunk vertices
        std::string level_names[MAX_LEVEL];
        for (uint32_t i = 0; i < MAX_LEVEL; ++i) level_names[i] = "level" + std::to_string(i);
        size_t resize[MAX_LEVEL];
        resize[0] = (chunks->get_max_chunkID() + 1) * chunks->get_chunk_size();
        resize[1] = chunks->get_max_offset();

        std::string SUFFIX[MAX_LEVEL];
        if (is_out_graph) {
            SUFFIX[0] = {".adj.chunk0"};
            SUFFIX[1] = {".adj.sv"};    
        } else {
            SUFFIX[0] = {".radj.chunk0"};
            SUFFIX[1] = {".radj.sv"};
        }
        
        for (uint32_t level = 0; level < MAX_LEVEL; ++level) {
            msync(sblk_pools[level]->get_base(), SBLK_POOL_SIZE / 2 / MAX_LEVEL, MS_SYNC);
            std::string filepath = SSDPATH + pool_name + level_names[level];
            int fd = open(filepath.c_str(), O_RDWR|O_CREAT, 00777);
            if (ftruncate(fd, resize[level]) == -1) {
                std::cout << "Could not ftruncate. error: " << strerror(errno) << std::endl;
                close(fd);
                exit(1);
            }
            std::string newname = SSDPATH + "/" + PREFIX + SUFFIX[level];
            if (rename(filepath.c_str(), newname.c_str())) {
                std::cout << "Could not rename. error: " << strerror(errno) << std::endl;
                exit(1);
            }
        }
    }

    vid_t* convert_addr(cpos_t cpos, degree_t degree) {
        if (degree <= threshold) {
            char* base = sblk_pools[0]->get_base();
            cpos_t chunk_id = cpos >> 32;
            cpos_t offset = cpos & 0xffffffff;
            return (vid_t*)(base + chunk_id * chunks->get_chunk_size() + offset);
        } else {
            char* base = sblk_pools[1]->get_base();
            return (vid_t*)(base + cpos);
        }
        return NULL;
    }

    cpos_t get_max_chunkID() {
        return chunks->get_max_chunkID();
    }

    size_t get_max_offset() {
        return chunks->get_max_offset();
    }

    size_t get_chunk_size() {
        return chunks->get_chunk_size();
    }

    degree_t get_threshold() {
        return threshold;
    }
    
private:
    trilevel_t* chunks;
    degree_t threshold; // threshold for hub vertices
};

class chunk_allocator_multilevel_t : public chunk_allocator_t {
    public:
    chunk_allocator_multilevel_t(bool _is_out_graph) : chunk_allocator_t() {
        is_out_graph = _is_out_graph;
        chunk_level = MAX_LEVEL - 1;
        threshold = new degree_t[chunk_level];
        // size_t chunk_sizes[2] = {4096, 2097152};
        size_t chunk_sizes[4] = {4*KB, 32*KB, 256*KB, 2*MB};
        for (uint32_t i = 0; i < chunk_level; ++i) {
            threshold[i] = chunk_sizes[i] / sizeof(vid_t);
        }
    }

    void init(std::string pool_name) {
        chunk_allocator_t::init(pool_name);
        chunks = new multilevel_t(sblk_pools, MAX_LEVEL);
    }

    uint32_t get_level(degree_t degree) {
        uint32_t level = 0;
        for (uint32_t i = 0; i < chunk_level; ++i) {
            if (degree <= threshold[i]) {
                level = i;
                break;
            }
        }
        return level;
    }

    cpos_t allocate(degree_t degree, vid_t** nebrs) {
        cpos_t cpos = 0;
        if (degree > threshold[chunk_level - 1]) {
            cpos = chunks->allocate_super(degree * sizeof(vid_t), nebrs);
        } else {
            uint32_t level = get_level(degree);
            cpos = chunks->allocate_medium(degree * sizeof(vid_t), nebrs, level);
        }
        return cpos;
    }

    void persist() {
        std::string pool_name = sblk_name + (is_out_graph ? "_out_" : "_in_");
        // persist chunk vertices
        std::string level_names[MAX_LEVEL];
        for (uint32_t i = 0; i < MAX_LEVEL; ++i) level_names[i] = "level" + std::to_string(i);
        size_t resize[MAX_LEVEL];
        for (uint32_t i = 0; i < MAX_LEVEL - 1; ++i) {
            resize[i] = (chunks->get_max_chunkID(i) + 1) * chunks->get_chunk_size(i);
        }
        resize[MAX_LEVEL - 1] = chunks->get_max_offset();

        std::string SUFFIX[MAX_LEVEL];
        if (is_out_graph) {
            for (uint32_t i = 0; i < MAX_LEVEL - 1; ++i) {
                SUFFIX[i] = {".adj.chunk" + std::to_string(i)};
            }
            SUFFIX[MAX_LEVEL - 1] = {".adj.sv"};
        } else {
            for (uint32_t i = 0; i < MAX_LEVEL - 1; ++i) {
                SUFFIX[i] = {".radj.chunk" + std::to_string(i)};
            }
            SUFFIX[MAX_LEVEL - 1] = {".radj.sv"};
        }
        
        for (uint32_t level = 0; level < MAX_LEVEL; ++level) {
            msync(sblk_pools[level]->get_base(), SBLK_POOL_SIZE / 2 / MAX_LEVEL, MS_SYNC);
            std::string filepath = SSDPATH + pool_name + level_names[level];
            int fd = open(filepath.c_str(), O_RDWR|O_CREAT, 00777);
            if (ftruncate(fd, resize[level]) == -1) {
                std::cout << "Could not ftruncate. error: " << strerror(errno) << std::endl;
                close(fd);
                exit(1);
            }
            std::string newname = SSDPATH + "/" + PREFIX + SUFFIX[level];
            if (rename(filepath.c_str(), newname.c_str())) {
                std::cout << "Could not rename. error: " << strerror(errno) << std::endl;
                exit(1);
            }
        }
    }

    vid_t* convert_addr(cpos_t cpos, degree_t degree) {
        if (degree <= threshold[chunk_level - 1]) {
            uint32_t level = get_level(degree);
            char* base = sblk_pools[level]->get_base();
            cpos_t chunk_id = cpos >> 32;
            cpos_t offset = cpos & 0xffffffff;
            return (vid_t*)(base + chunk_id * chunks->get_chunk_size(level) + offset);
        } else {
            char* base = sblk_pools[MAX_LEVEL-1]->get_base();
            return (vid_t*)(base + cpos);
        }
        return NULL;
    }

    cpos_t get_max_chunkID(uint32_t level) {
        return chunks->get_max_chunkID(level);
    }

    size_t get_max_offset() {
        return chunks->get_max_offset();
    }

    size_t get_chunk_size(uint32_t level) {
        return chunks->get_chunk_size(level);
    }

    degree_t get_threshold(uint32_t level) {
        return threshold[level];
    }
    void print_fragment() {
        chunks->print_fragment();
    
    }
    
private:
    multilevel_t* chunks;
    uint32_t chunk_level;
    degree_t* threshold; // threshold for hub vertices
};