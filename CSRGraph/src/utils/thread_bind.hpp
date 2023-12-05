#pragma once

#include <iostream>
#include <unistd.h>
#include <sys/syscall.h>
#include <thread>

#define gettid() syscall(__NR_gettid)

inline void bind_thread_to_cpu(int cpu_id){
    cpu_set_t cpu_mask;
    CPU_ZERO(&cpu_mask);
    CPU_SET(cpu_id, &cpu_mask);
    // sched_setaffinity(gettid(), sizeof(cpu_mask), &cpu_mask);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_mask), &cpu_mask);
}

inline void cancel_thread_bind(){
    uint8_t cpu_num = std::thread::hardware_concurrency();
    // std::cout << "In cancel_thread_bind(), cpu_num = " << cpu_num << std::endl; // 96
    cpu_set_t cpu_mask;
    CPU_ZERO(&cpu_mask);
    for(uint8_t cpu_id = 0; cpu_id < cpu_num; cpu_id++)
        CPU_SET(cpu_id, &cpu_mask);
    // sched_setaffinity(gettid(), sizeof(cpu_mask), &cpu_mask);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_mask), &cpu_mask);
}

inline void bind_thread_to_socket(tid_t tid, int socket_id){
    // assert(NUM_SOCKETS == 2);
    if(socket_id == 0){
        if(tid >= 0 && tid < 24) bind_thread_to_cpu(tid);
        else if(tid < 48) bind_thread_to_cpu(tid + 24);
        // else cancel_thread_bind(); // only 48 cores available, other threads need to access PM across NUMA node
    } else if(socket_id == 1){
        if(tid >= 0 && tid < 24) bind_thread_to_cpu(tid + 72);
        else if(tid < 48) bind_thread_to_cpu(tid);
        else cancel_thread_bind(); // only 48 cores available, other threads need to access PM across NUMA node
    } else {
        std::cout << "Wrong socket id: " << socket_id << std::endl;
        exit(1);
    }
}

inline void bind_thread_to_socket0(tid_t tid, int socket_id){
    // assert(NUM_SOCKETS == 2);
    if(socket_id == 0){
        if(tid < 24 || (tid >= 48 && tid < 72)) bind_thread_to_cpu(tid);
        else bind_thread_to_cpu(tid-24);
    } else if(socket_id == 1){
        if(tid < 24 || (tid >= 48 && tid < 72)) bind_thread_to_cpu(tid+24);
        else bind_thread_to_cpu(tid);
    } else {
        std::cout << "Wrong socket id: " << socket_id << std::endl;
        exit(1);
    }
}

inline uint8_t GET_SOCKETID(vid_t vid){
    // assert(NUM_SOCKETS == 2);
    return vid & 1;
    // return vid % NUM_SOCKETS;
}
