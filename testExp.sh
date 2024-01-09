#!/bin/bash

USE_CHUNK=1
# 0: ligra-mmap; 1: ligra-chunk

[ $USE_CHUNK -eq 1 ] && export CHUNK=1

[ $USE_CHUNK -eq 1 ] && DATA_PATH=/mnt/nvme1/zorax/chunks/ || DATA_PATH=/mnt/nvme1/zorax/datasets/
CGROUP_PATH=/sys/fs/cgroup/memory/chunkgraph/
# CPU:use NUMA 0 node, with id 0-23 and 48-71, with taskset command
# TEST_CPU_SET="taskset --cpu-list 0-95:1"
TEST_CPU_SET="taskset -c 0-23,48-71:1"

PERF_CACHE_MISS=1
[ $PERF_CACHE_MISS -eq 1 ] && export CACHEMISS=1

export OMP_PROC_BIND=true

name[0]=twitter
name[1]=friendster
name[2]=ukdomain
name[3]=kron28
name[4]=kron29
name[5]=kron30
name[6]=yahoo

chunkgraph=true

[ $USE_CHUNK -eq 1 ] && data[0]=${DATA_PATH}twitter/${name[0]} || data[0]=${DATA_PATH}csr_bin/Twitter/${name[0]}
[ $USE_CHUNK -eq 1 ] && data[1]=${DATA_PATH}friendster/${name[1]} || data[1]=${DATA_PATH}csr_bin/Friendster/${name[1]}
[ $USE_CHUNK -eq 1 ] && data[2]=${DATA_PATH}ukdomain/${name[2]} || data[2]=${DATA_PATH}csr_bin/Ukdomain/${name[2]}
[ $USE_CHUNK -eq 1 ] && data[3]=${DATA_PATH}kron28/${name[3]} || data[3]=${DATA_PATH}csr_bin/Kron28/${name[3]}
[ $USE_CHUNK -eq 1 ] && data[4]=${DATA_PATH}kron29/${name[4]} || data[4]=${DATA_PATH}csr_bin/Kron29/${name[4]}
[ $USE_CHUNK -eq 1 ] && data[5]=${DATA_PATH}kron30/${name[5]} || data[5]=${DATA_PATH}csr_bin/Kron30/${name[5]}
[ $USE_CHUNK -eq 1 ] && data[6]=${DATA_PATH}yahoo/${name[6]} || data[6]=${DATA_PATH}csr_bin/Yahoo/${name[6]}

# roots:        TT   FS     UK       K28       K29       K30       YW       K31       CW
declare -a rts=(12 801109 5699262 254655025 310059974 233665123 35005211 691502068 739935047)
declare -a reorder_rts=(0 0 0 0 0 0 0)
declare -a kcore_iter=(10 10 10 10 10 10 3)

function set_schedule {
	SCHEDULE=$1
	export OMP_SCHEDULE="${SCHEDULE}"
}

set_schedule "dynamic"

clear_pagecaches() { 
    echo "zwx.1005" | sudo -S sysctl -w vm.drop_caches=3;
}

get_total_bytes_read_iostat() {
    eval beg_file="$1"
    eval end_file="$2"
    DEVICE="nvme0n1"
    beg=$(cat ${beg_file} | grep ${DEVICE} | awk '{print $6}')
    end=$(cat ${end_file} | grep ${DEVICE} | awk '{print $6}')
    echo "$end - $beg" | bc
}

not_save_detail_log=true

profile_diskio() {
    eval filename="$1"
    DEVICE="/dev/nvme0n1"
    time_slot=1

    iostat -d ${time_slot} ${DEVICE} > ../results/${filename}.iostat & 
}

profile_memory() {
    cgroup_limit=true
    eval commandargs="$1"
    eval filename="$2"
    eval limit="$3"

    commandname=$(echo $commandargs | awk '{print $1}')

    commandargs="${TEST_CPU_SET} ${commandargs}"

    cur_time=$(date "+%Y-%m-%d %H:%M:%S")

    log_dir="../results/logs/${log_time}"

    echo $cur_time "profile run with command: " $commandargs >> ../results/command.log
    echo $cur_time "profile run with command: " $commandargs > ${log_dir}/${filename}.txt

    echo "memory bound: " $limit "GB" >> ../results/command.log
    echo "memory bound: " $limit "GB" >> ${log_dir}/${filename}.txt

    nohup $commandargs &>> ${log_dir}/${filename}.txt &
    pid=$(ps -ef | grep $commandname | grep -v grep | awk '{print $2}')

    echo "pid: " $pid >> ../results/command.log

    # # debug
    # sudo perf record -e cpu-clock,cache-misses -g -p $pid -o ${log_dir}/${filename}.perf.data &

    if $cgroup_limit; then
        echo $pid > ${CGROUP_PATH}/cgroup.procs
        echo "cgroup tasks:" $(cat ${CGROUP_PATH}tasks) >> ../results/command.log
    fi

    # monitor and record the amount of physical memory used for the process pid

    echo "monitoring memory usage for pid:" $pid > ${log_dir}/${filename}.memory
    echo "VmSize VmRSS VmSwap" >> ${log_dir}/${filename}.memory
    maxVmPeak=0
    maxVmHWM=0
    while :
    do
        pid=$(ps -ef | grep $commandname | grep -v grep | awk '{print $2}')
        if [ -z "$pid" ]; then
            break
        fi
        cat /proc/$pid/status | grep -E "VmSize|VmRSS|VmSwap" | awk '{printf("%s ", $2)}' >> ${log_dir}/${filename}.memory
        echo "" >> ${log_dir}/${filename}.memory
        # record and refresh the newest VmPeak VmHWM
        VmPeak=$(cat /proc/$pid/status | grep -E "VmPeak" | awk '{print $2}')
        VmHWM=$(cat /proc/$pid/status | grep -E "VmHWM" | awk '{print $2}')
        if [ $VmPeak -gt $maxVmPeak ]; then
            maxVmPeak=$VmPeak
        fi
        if [ $VmHWM -gt $maxVmHWM ]; then
            maxVmHWM=$VmHWM
        fi
        sleep 0.1
    done
    echo "maxVmPeak: " $maxVmPeak >> ${log_dir}/${filename}.memory
    echo "maxVmHWM: " $maxVmHWM >> ${log_dir}/${filename}.memory
    echo "Peak virtual memory size: " $maxVmPeak "KB ("$(echo "scale=2; $maxVmPeak/1024/1024" | bc) "GB)" >> ${log_dir}/${filename}.txt
    echo "Peak resident set size: " $maxVmHWM "KB ("$(echo "scale=2; $maxVmHWM/1024/1024" | bc) "GB)" >> ${log_dir}/${filename}.txt

    wait $pid

    res=$(awk 'NR>5 {sum+=$5} END {printf "%.0f\n", sum}' ${log_dir}/${filename}.diskio)
    # echo total bytes read in KB and convert to GB
    echo "total bytes read during compute: " $res "KB ("$(echo "scale=2; $res/1024/1024" | bc) "GB)" >> ${log_dir}/${filename}.txt
    echo "total bytes read during compute: " $res "KB ("$(echo "scale=2; $res/1024/1024" | bc) "GB)" >> ${log_dir}/${filename}.diskio

    sleep 1s
    echo >> ${log_dir}/${filename}.txt
    cat ${log_dir}/${filename}.cachemiss >> ${log_dir}/${filename}.txt

    if $not_save_detail_log; then
        rm -f ${log_dir}/${filename}.cachemiss ${log_dir}/${filename}.memory ${log_dir}/${filename}.diskio ${log_dir}/${filename}.iostat
    fi
}

profile_performance() {
    eval commandargs="$1"
    eval filename="$2"

    commandname=$(echo $commandargs | awk '{print $1}')

    commandargs="${TEST_CPU_SET} ${commandargs}"

    cur_time=$(date "+%Y-%m-%d %H:%M:%S")

    log_dir="../results/logs/${log_time}"

    echo $cur_time "profile run with command: " $commandargs >> ../results/command.log
    echo $cur_time "profile run with command: " $commandargs > ${log_dir}/${filename}.txt

    nohup $commandargs &>> ${log_dir}/${filename}.txt &
    pid=$(ps -ef | grep $commandname | grep -v grep | awk '{print $2}')

    echo "pid: " $pid >> ../results/command.log

    wait $pid

    res=$(awk 'NR>5 {sum+=$5} END {printf "%.0f\n", sum}' ${log_dir}/${filename}.diskio)
    # echo total bytes read in KB and convert to GB
    echo "total bytes read during compute: " $res "KB ("$(echo "scale=2; $res/1024/1024" | bc) "GB)" >> ${log_dir}/${filename}.txt
    echo "total bytes read during compute: " $res "KB ("$(echo "scale=2; $res/1024/1024" | bc) "GB)" >> ${log_dir}/${filename}.diskio

    sleep 1s
    echo >> ${log_dir}/${filename}.txt
    cat ${log_dir}/${filename}.cachemiss >> ${log_dir}/${filename}.txt

    if $not_save_detail_log; then
        rm -f ${log_dir}/${filename}.cachemiss ${log_dir}/${filename}.diskio ${log_dir}/${filename}.iostat
    fi
}


if $chunkgraph; then
    log_time=$(date "+%Y%m%d_%H%M%S")
    mkdir -p results/logs/${log_time}
    cd apps && make clean

    outputFile="../results/hierg_query_time.csv"
    title="ChunkGraph"
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test ${title} Query Performace" >> ${outputFile}
    
    bounds=('4 6 8 12 16 256'   # twitter
            '4 6 8 12 16 256'   # friendster
            '4 8 12 16 20 256'  # ukdomain
            '12 16 24 32 40 256'    # kron28
            '32 48 56 64 80 256'    # kron29
            '64 80 96 256'  # kron30
            '48 56 64 72 80 96 256')    # yahoo

    bounds=('8 12 16 256'   # twitter
            '8 12 16 256'   # friendster
            '6 8 16 20 256'  # ukdomain
            '12 16 24 32 40 256'    # kron28
            '32 48 56 64 80 256'    # kron29
            '64 80 96 256'  # kron30
            '48 56 64 72 80 96 256')    # yahoo

    bounds=('256'
            '256'
            '256'
            '256'
            '256'
            '256'
            '256')

    for idx in {0,1,2,3,4,5,6};
    # for idx in 6;
    do
        echo -n "Data: "
        echo ${data[$idx]}
        echo -n "Root: "
        echo ${rts[$idx]}

        base_bound=${bounds[$idx]}
        base_bound=($base_bound)
        echo -n "Base Bound: "
        echo ${base_bound[@]}
        len=${#base_bound[@]}
        for ((i=0;i<$len;i++))
        do
            memory_bound[$i]=$((${base_bound[$i]}*1024*1024*1024))
        done
        echo -n "Memory Bound: "
        echo ${memory_bound[@]}

        make BFS
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./BFS -b -r ${rts[$idx]} -chunk -threshold 20 -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bfs_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make BC
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./BC -b -r ${rts[$idx]} -chunk -threshold 20 -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make PageRank
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./PageRank -b -chunk -threshold 20 -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_pr_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make Components
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./Components -b -chunk -threshold 20 -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_cc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done
        
        make KCore
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./KCore -b -chunk -threshold 20 -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_kc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make Radii
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./Radii -b -chunk -threshold 20 -buffer ${base_bound[$mem]} ${data[${idx}]} "
            filename="${name[${idx}]}_chunk_radii_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make MIS
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./MIS -b -chunk -threshold 20 -buffer ${base_bound[$mem]} ${data[${idx}]} "
            filename="${name[${idx}]}_chunk_mis_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done
    done
fi