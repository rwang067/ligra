#pragma once

#include <iostream>
#include <cstring>
#include "common.hpp"
#include "args.hpp"

size_t fsize(const std::string& fname){
    struct stat st;
    if (0 == stat(fname.c_str(), &st)) {
        return st.st_size;
    }
    perror("stat issue");
    return -1L;
}

/* -------------------------------------------------------------- */
// ALLOC in SSD -- use mmap
static inline void* ssd_alloc(const char* filepath, size_t size){
    int fd = open(filepath, O_RDWR|O_CREAT, 00777);
    if(fd == -1){
      std::cout << "Could not open file for :" << filepath << " error: " << strerror(errno) << std::endl;
      exit(1);
    }
    if(ftruncate(fd, size) == -1){
      std::cout << "Could not ftruncate file for :" << filepath << " error: " << strerror(errno) << std::endl;
      close(fd);
      exit(1);
    }
    char* addr = (char*)mmap(NULL, size, PROT_READ|PROT_WRITE,MAP_SHARED, fd, 0);
    close(fd);
    if (addr == (char*)MAP_FAILED) {
        std::cout << "Could not mmap for :" << filepath << " error: " << strerror(errno) << std::endl;
        std::cout << "size = " << size << std::endl;
        exit(1);
    }
    // memset(addr, 0, size);  //pre touch, 消除page fault影响
    // if(msync(addr, size, MS_SYNC) == -1){
    //   perror("Msync fail:");
    //   exit(1);
    // }
    return addr;
}

void alloc_local_buf(size_t size, char** buf, std::string suffix = "txt") {
    std::string local_buf_file = SSDPATH + "local_buf." + suffix;
    void* local_buf = 0;
    local_buf = (char*)ssd_alloc(local_buf_file.c_str(), size);
    *buf = (char*)local_buf;
    std::cout << "Allocated " << size * 1.0 / GB << "GB for local_buf." << std::endl;
}

size_t alloc_and_read_file(std::string filepath, char** buf, size_t fsz = 0) {
    size_t size = fsz ? fsz : fsize(filepath);
    alloc_local_buf(size, buf, filepath.substr(filepath.rfind('.')+1));
    FILE* file = fopen(filepath.c_str(), "rb");
    std::cout << "filepath = " << filepath << std::endl;
    assert(file != 0);
    if (fsize(filepath) != fread((void*)*buf, sizeof(char), size, file)) {
        std::cout << "Read wrong size!" << std::endl;
        assert(0);
    }
    return size;
}

void free_local_buf(size_t size, char** buf, std::string suffix = "txt") {
    void* local_buf = *buf;
    if(!local_buf) return ;
    munmap(local_buf, size);
    std::string local_buf_file = SSDPATH + "local_buf." + suffix;
    unlink(local_buf_file.c_str());
    *buf = 0;
    std::cout << "Freed " << size * 1.0 / GB << "GB for local_buf." << std::endl;
}