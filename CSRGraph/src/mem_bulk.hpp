#pragma once
#include "mempool.hpp"
#include "common.hpp"


/* ---------------------------------------------------------------------- */
// memory allocator for vertex
struct mem_bulk_t{
    vertex_t* vertex_bulk;
    vid_t vert_count;
};

class thd_mem_t {
private:
    mem_bulk_t* mem; 
public:
    inline thd_mem_t() {
        if(posix_memalign((void**)&mem, 64, THD_COUNT * sizeof(mem_bulk_t))) {
            std::cout << "posix_memalign failed()" << std::endl;
            mem = (mem_bulk_t*)calloc(sizeof(mem_bulk_t), THD_COUNT);
        } else {
            memset(mem, 0, THD_COUNT*sizeof(mem_bulk_t));
        }
    }

    ~thd_mem_t() {
        if(mem) free(mem);
    }
    
    inline vertex_t* new_vertex() {
        mem_bulk_t* mem1 = mem + omp_get_thread_num();  
		if (mem1->vert_count == 0) {
            index_t count = VERT_BULK_SIZE;
            mem1->vert_count = count;
            mem1->vertex_bulk = (vertex_t*)calloc(sizeof(vertex_t), count);
		}
		mem1->vert_count--;
		return mem1->vertex_bulk++;
	}
};

/* ---------------------------------------------------------------------- */
// memory allocator for chunk

class chunk_t {
protected:
    mempool_t** sblk_pools;
public:
    chunk_t();
    chunk_t(mempool_t** _sblk_pools) : sblk_pools(_sblk_pools) {}
};

class csrchunk_t : public chunk_t {
private:

    struct sblk_bulk_t {
        char* addr;
        size_t size;
        char* chunk;
        size_t current_csize;

        inline void init() {
            addr = NULL;
            size = 0;
            chunk = NULL;
            current_csize = 0;
        }
    };

public:
    csrchunk_t();
    csrchunk_t(mempool_t** _sblk_pools) : chunk_t(_sblk_pools) {
        sblk_bulk = (sblk_bulk_t*)calloc(sizeof(sblk_bulk_t), THD_COUNT);
        max_csize = 4 * KB;

        for(tid_t tid = 0; tid < THD_COUNT; ++tid) {
            sblk_bulk[tid].init();
        }
    }

    void allocate_new_bulk() {
        sblk_bulk_t* bulk = sblk_bulk + omp_get_thread_num();
        size_t remain_size = sblk_pools[0]->get_remained();
        if (remain_size < MEM_BULK_SIZE) {
            logstream(LOG_DEBUG) << "No remaining space in sblk pool!" << std::endl;
            sblk_pools[0]->print_usage();
            exit(0);
        }
        bulk->addr = (char*)sblk_pools[0]->alloc(MEM_BULK_SIZE);
        if (bulk->addr == NULL) {
            logstream(LOG_DEBUG) << "Fail to allocate space in sblk pool!" << std::endl;
            sblk_pools[0]->print_usage();
            exit(0);
        }
        bulk->size = MEM_BULK_SIZE;
    }

    void allocate_new_chunk(size_t size) {
        sblk_bulk_t* bulk = sblk_bulk + omp_get_thread_num();
        if (bulk->size < size) {
            // logstream(LOG_DEBUG) << "No remaining space in the current sblk bulk!" << std::endl;
            // if (size > MEM_BULK_SIZE) {
            //     logstream(LOG_DEBUG) << "The chunk size is too large!" << std::endl;
            //     exit(0);
            // }
            allocate_new_bulk();
        }
        // assert(bulk->addr != NULL);
        bulk->chunk = bulk->addr;
        bulk->current_csize = 0;
        bulk->addr += size;
        bulk->size -= size;
    }

    inline cpos_t allocate_super(size_t vsize, vid_t** adjlist) {
        sblk_bulk_t* bulk = sblk_bulk + omp_get_thread_num();
        
        // allocate new chunk to align
        allocate_new_chunk(max_csize);

        cpos_t chunk_id = (bulk->chunk - sblk_pools[0]->get_base()) / max_csize;
        cpos_t offset = bulk->current_csize;
        *adjlist = (vid_t*)((char*)bulk->chunk + offset);

        size_t remain_size = vsize - max_csize;
        while (remain_size > max_csize) {
            allocate_new_chunk(max_csize);
            remain_size -= max_csize;
        }
        allocate_new_chunk(max_csize);

        bulk->current_csize = remain_size;
        return (chunk_id << 32) | offset;
    }

    inline cpos_t allocate_medium(size_t vsize, vid_t** adjlist) {
        sblk_bulk_t* bulk = sblk_bulk + omp_get_thread_num();
        
        if (bulk->chunk == NULL || bulk->current_csize + vsize > max_csize) {
            allocate_new_chunk(max_csize);
        }

        cpos_t chunk_id = (bulk->chunk - sblk_pools[0]->get_base()) / max_csize;
        cpos_t offset = bulk->current_csize;
        *adjlist = (vid_t*)((char*)bulk->chunk + offset);

        bulk->current_csize += vsize;
        return (chunk_id << 32) | offset;
    }

    size_t get_chunk_size() {
        return max_csize;
    }

    cpos_t get_max_chunkID() {
        cpos_t max_chunkID = 0;
        for(tid_t tid = 0; tid < THD_COUNT; ++tid) {
            sblk_bulk_t* bulk = sblk_bulk + tid;
            if (bulk->chunk == NULL) continue;
            cpos_t chunkID = (bulk->chunk - sblk_pools[0]->get_base()) / max_csize;
            if (chunkID > max_chunkID) max_chunkID = chunkID;
            std::cout << "tid = " << tid << ", chunkID = " << chunkID << std::endl;
        }
        return max_chunkID;
    }

private:
    sblk_bulk_t* sblk_bulk;
    size_t max_csize;
};

class trilevel_t : public chunk_t {
private:

    struct sblk_bulk_t {
        char* addr;
        size_t size;
        char* chunk;
        size_t current_csize;

        inline void init() {
            addr = NULL;
            size = 0;
            chunk = NULL;
            current_csize = 0;
        }
    };

    struct hub_bulk_t {
        char* addr;
        size_t size;

        inline void init() {
            addr = NULL;
            size = 0;
        }
    };

public:
    trilevel_t();
    trilevel_t(mempool_t** _sblk_pools) : chunk_t(_sblk_pools) {
        sblk_bulk = (sblk_bulk_t*)calloc(sizeof(sblk_bulk_t), THD_COUNT);
        hub_bulk = (hub_bulk_t*)calloc(sizeof(hub_bulk_t), THD_COUNT);
        max_csize = 4 * KB;

        for(tid_t tid = 0; tid < THD_COUNT; ++tid) {
            sblk_bulk[tid].init();
            hub_bulk[tid].init();
        }
    }

    void allocate_new_bulk() {
        sblk_bulk_t* bulk = sblk_bulk + omp_get_thread_num();
        size_t remain_size = sblk_pools[0]->get_remained(); 
        if (remain_size < MEM_BULK_SIZE) {
            logstream(LOG_DEBUG) << "No remaining space in sblk pool!" << std::endl;
            sblk_pools[0]->print_usage();
            exit(0);
        }
        bulk->addr = (char*)sblk_pools[0]->alloc(MEM_BULK_SIZE);
        if (bulk->addr == NULL) {
            logstream(LOG_DEBUG) << "Fail to allocate space in sblk pool!" << std::endl;
            sblk_pools[0]->print_usage();
            exit(0);
        }
        bulk->size = MEM_BULK_SIZE;
    }

    void allocate_new_chunk(size_t size) {
        sblk_bulk_t* bulk = sblk_bulk + omp_get_thread_num();
        if (bulk->size < size) {
            // logstream(LOG_DEBUG) << "No remaining space in the current sblk bulk!" << std::endl;
            // if (size > MEM_BULK_SIZE) {
            //     logstream(LOG_DEBUG) << "The chunk size is too large!" << std::endl;
            //     exit(0);
            // }
            allocate_new_bulk();
        }
        // assert(bulk->addr != NULL);
        bulk->chunk = bulk->addr;
        bulk->current_csize = 0;
        bulk->addr += size;
        bulk->size -= size;
    }

    inline void allocate_hub(size_t size) {
        size_t remain_size = sblk_pools[1]->get_remained();
        if (remain_size < size) {
            logstream(LOG_DEBUG) << "No remaining space in hub pool!" << std::endl;
            sblk_pools[1]->print_usage();
            exit(0);
        }
        hub_bulk_t* hub = hub_bulk + omp_get_thread_num();
        hub->addr = (char*)sblk_pools[1]->alloc(size);
        if (hub->addr == NULL) {
            logstream(LOG_DEBUG) << "Fail to allocate space in hub pool!" << std::endl;
            sblk_pools[1]->print_usage();
            exit(0);
        }
        hub->size = size;
    }

    inline cpos_t allocate_super(size_t vsize, vid_t** adjlist) {
        hub_bulk_t* hub = hub_bulk + omp_get_thread_num();
        if (hub->addr == NULL || hub->size < vsize) {
            allocate_hub(vsize);
        }
        *adjlist = (vid_t*)(hub->addr);
        hub->size -= vsize;
        cpos_t offset = hub->addr - sblk_pools[1]->get_base();
        hub->addr += vsize;
        return offset;
    }

    inline cpos_t allocate_medium(size_t vsize, vid_t** adjlist) {
        sblk_bulk_t* bulk = sblk_bulk + omp_get_thread_num();
        
        if (bulk->chunk == NULL || bulk->current_csize + vsize > max_csize) {
            allocate_new_chunk(max_csize);
        }

        cpos_t chunk_id = (bulk->chunk - sblk_pools[0]->get_base()) / max_csize;
        cpos_t offset = bulk->current_csize;
        *adjlist = (vid_t*)((char*)bulk->chunk + offset);

        bulk->current_csize += vsize;
        return (chunk_id << 32) | offset;
    }

    size_t get_chunk_size() {
        return max_csize;
    }

    cpos_t get_max_chunkID() {
        cpos_t max_chunkID = 0;
        for(tid_t tid = 0; tid < THD_COUNT; ++tid) {
            sblk_bulk_t* bulk = sblk_bulk + tid;
            if (bulk->chunk == NULL) continue;
            cpos_t chunkID = (bulk->chunk - sblk_pools[0]->get_base()) / max_csize;
            if (chunkID > max_chunkID) max_chunkID = chunkID;
            // std::cout << "tid = " << tid << ", chunkID = " << chunkID << std::endl;
        }
        return max_chunkID;
    }

    cpos_t get_max_offset() {
        cpos_t max_offset = 0;
        for(tid_t tid = 0; tid < THD_COUNT; ++tid) {
            hub_bulk_t* bulk = hub_bulk + tid;
            if (bulk->addr == NULL) continue;
            cpos_t offset = bulk->addr - sblk_pools[1]->get_base();
            if (offset > max_offset) max_offset = offset;
            // std::cout << "tid = " << tid << ", offset = " << offset << std::endl;
        }
        return max_offset;
    }

private:
    sblk_bulk_t* sblk_bulk;
    hub_bulk_t* hub_bulk;
    size_t max_csize;
};