#pragma once

#include "common.hpp"

struct vertex_t {
public:
    degree_t out_deg;
    uint64_t residue;

    inline degree_t get_out_degree(){ return out_deg; }
    inline void set_out_degree(degree_t _out_deg ){ out_deg = _out_deg; }

    inline vid_t* get_csr(){ return (vid_t*)residue;}
    inline void set_csr(vid_t* block){ residue = (uint64_t)block;}

    inline cpos_t get_cpos(){ return (cpos_t)residue;}
    inline void set_cpos(cpos_t cpos){ residue = (uint64_t)cpos;}

    // case 1: use nb1, nb2
    inline vid_t* get_nebrs() { return (vid_t*)(&residue); }
    inline void add_nebr(vid_t nebr) {
        if(residue == 0) {
            residue = (residue | (uint64_t)nebr);
        } else {
            residue = (residue | ((uint64_t)nebr << 32));
        }
    }

    inline void set_minivertex(uint64_t minivertex) { residue = minivertex; }
    inline uint64_t get_minivertex() { return (uint64_t)(residue); }
};