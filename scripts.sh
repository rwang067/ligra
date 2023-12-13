#!/bin/bash

USE_CHUNK=0
# 0: ligra-mmap; 1: ligra-chunk

[ $USE_CHUNK -eq 1 ] && export CHUNK=1

[ $USE_CHUNK -eq 1 ] && DATA_PATH=/mnt/nvme1/zorax/chunks/ || DATA_PATH=/mnt/nvme1/zorax/datasets/
CGROUP_PATH=/sys/fs/cgroup/memory/chunkgraph/
TEST_CPU_SET="taskset --cpu-list 49-96:1"

export OMP_PROC_BIND=true

name[0]=twitter
name[1]=friendster
name[2]=ukdomain
name[3]=kron28
name[4]=kron29
name[5]=kron30
name[6]=yahoo

[ $USE_CHUNK -eq 1 ] && data[0]=${DATA_PATH}twitter/${name[0]} || data[0]=${DATA_PATH}csr_bin/Twitter/${name[0]}
[ $USE_CHUNK -eq 1 ] && data[1]=${DATA_PATH}friendster/${name[1]} || data[1]=${DATA_PATH}csr_bin/Friendster/${name[1]}
[ $USE_CHUNK -eq 1 ] && data[2]=${DATA_PATH}ukdomain/${name[2]} || data[2]=${DATA_PATH}csr_bin/Ukdomain/${name[2]}
[ $USE_CHUNK -eq 1 ] && data[3]=${DATA_PATH}kron28/${name[3]} || data[3]=${DATA_PATH}csr_bin/Kron28/${name[3]}
[ $USE_CHUNK -eq 1 ] && data[4]=${DATA_PATH}kron29/${name[4]} || data[4]=${DATA_PATH}csr_bin/Kron29/${name[4]}
[ $USE_CHUNK -eq 1 ] && data[5]=${DATA_PATH}kron30/${name[5]} || data[5]=${DATA_PATH}csr_bin/Kron30/${name[5]}
[ $USE_CHUNK -eq 1 ] && data[6]=${DATA_PATH}yahoo/${name[6]} || data[6]=${DATA_PATH}csr_bin/Yahoo/${name[6]}

# roots:        TT   FS     UK       K28       K29       K30       YW       K31       CW
declare -a rts=(12 801109 5699262 254655025 310059974 233665123 35005211 691502068 739935047)
# declare -a rts=(18225348 26737282 5699262 254655025 310059974 233665123 35005211 691502068 739935047)
declare -a base_bound=(8 12 16 128)

function set_schedule {
	SCHEDULE=$1
	export OMP_SCHEDULE="${SCHEDULE}"
}

set_schedule "dynamic"

clear_pagecaches() { 
    echo "zwx.1005" | sudo -S sysctl -w vm.drop_caches=3;
}

profile_diskio() {
    eval filename="$1"
    DEVICE="/dev/nvme0n1"
    time_slot=1

    iostat -d ${time_slot} ${DEVICE} > ../results/${filename}.iostat & 
}

log_time=$(date "+%Y%m%d_%H%M%S")
mkdir -p results/logs/${log_time}
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

    nohup $commandargs >> ${log_dir}/${filename}.txt &
    pid=$(ps -ef | grep $commandname | grep -v grep | awk '{print $2}')

    echo "pid: " $pid >> ../results/command.log

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
}

profile_performance() {
    cgroup_limit=true
    eval commandargs="$1"
    eval filename="$2"
    eval limit="$3"

    commandname=$(echo $commandargs | awk '{print $1}')

    commandargs="${TEST_CPU_SET} ${commandargs}"

    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "profile run with command: " $commandargs >> ../results/command.log
    echo $cur_time "profile run with command: " $commandargs > ../results/logs/${log_time}/${filename}.txt

    echo "memory bound: " $limit "GB" >> ../results/command.log
    echo "memory bound: " $limit "GB" >> ../results/logs/${log_time}/${filename}.txt

    nohup $commandargs >> ../results/logs/${log_time}/${filename}.txt &
    pid=$(ps -ef | grep $commandname | grep -v grep | awk '{print $2}')

    echo "pid: " $pid >> ../results/command.log

    if $cgroup_limit; then
        echo $pid > ${CGROUP_PATH}/cgroup.procs
        echo "cgroup tasks:" $(cat ${CGROUP_PATH}tasks) >> ../results/command.log
    fi
    wait $pid
}

debug=false
run_performance=false
cgroup_swap=true

# cd apps && make TestAll
# cd apps && make clean && make BFS PageRank Components KCore

if $debug; then
    cd apps && make clean && make BFS
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test Ligra-swap Query Performace"
    for idx in 0;
    do
        echo -n "Data: "
        echo ${data[$idx]}
        echo -n "Root: "
        echo ${rts[$idx]}
        echo -n "Name: "
        echo ${name[$idx]}
        # clear_pagecaches
        ./BFS -b -r ${rts[$idx]} -chunk ${data[${idx}]}
        # gdb --args ./testNebrs -b -chunk -debug ${data[${idx}]}
    done
fi

if $run_performance; then
    cd apps && make clean && make testNebrs BFS BC PageRank Components KCore Radii
    
    memory_bound[0]=$((${base_bound[0]}*1024*1024*1024))
    memory_bound[1]=$((${base_bound[1]}*1024*1024*1024))
    memory_bound[2]=$((${base_bound[2]}*1024*1024*1024))

    outputFile="../results/hierg_query_time.csv"
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test ChunkGraph-swap Query Performace" >> ${outputFile}
    
    for idx in {0,1};
    do
        echo -n "Data: "
        echo ${data[$idx]}
        echo -n "Root: "
        echo ${rts[$idx]}

        for mem in {0,1,2,3};
        do
            clear_pagecaches

            commandargs="./BFS -b -r ${rts[$idx]} -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bfs_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            profile_performance "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        for mem in {0,1,2,3};
        do
            clear_pagecaches
            
            commandargs="./BC -b -r ${rts[$idx]} -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            profile_performance "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        for mem in {0,1,2,3};
        do
            clear_pagecaches

            commandargs="./PageRank -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_pr_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            profile_performance "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        for mem in {0,1,2,3};
        do
            clear_pagecaches

            commandargs="./Components -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_pr_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            profile_performance "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        for mem in {0,1,2,3};
        do
            clear_pagecaches

            commandargs="./KCore -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_pr_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            profile_performance "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        for mem in {0,1,2,3};
        do
            clear_pagecaches

            commandargs="./Radii -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_pr_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            profile_performance "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done
    done
fi

if $cgroup_swap; then
    # cd apps && make clean && make testNebrs BFS BC PageRank Components KCore Radii
    # exit
    cd apps && make clean
    
    memory_bound[0]=$((${base_bound[0]}*1024*1024*1024))
    memory_bound[1]=$((${base_bound[1]}*1024*1024*1024))
    memory_bound[2]=$((${base_bound[2]}*1024*1024*1024))
    memory_bound[3]=$((${base_bound[3]}*1024*1024*1024))

    [ $USE_CHUNK -eq 1 ] && outputFile="../results/hierg_query_time.csv" || outputFile="../results/ligra_mmap_query_time.csv"
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test ChunkGraph-swap Query Performace" >> ${outputFile}
    for idx in {0,1};
    # for idx in 1;
    do
        echo -n "Data: "
        echo ${data[$idx]}
        echo -n "Root: "
        echo ${rts[$idx]}

        make BFS
        for mem in {0,1,2,3};
        # for mem in 0;
        do
            clear_pagecaches
            [ $USE_CHUNK -eq 1 ] && commandargs="./BFS -b -r ${rts[$idx]} -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}" || commandargs="./BFS -b -r ${rts[$idx]} ${data[${idx}]}"
            [ $USE_CHUNK -eq 1 ] && filename="${name[${idx}]}_chunk_bfs_${base_bound[$mem]}" || filename="${name[${idx}]}_mmap_bfs_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make BC
        for mem in {0,1,2,3};
        # for mem in {2,3};
        do
            clear_pagecaches
            [ $USE_CHUNK -eq 1 ] && commandargs="./BC -b -r ${rts[$idx]} -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}" || commandargs="./BC -b -r ${rts[$idx]} ${data[${idx}]}"
            [ $USE_CHUNK -eq 1 ] && filename="${name[${idx}]}_chunk_bc_${base_bound[$mem]}" || filename="${name[${idx}]}_mmap_bc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make PageRank
        for mem in {0,1,2,3};
        do
            clear_pagecaches
            [ $USE_CHUNK -eq 1 ] && commandargs="./PageRank -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}" || commandargs="./PageRank -b ${data[${idx}]}"
            [ $USE_CHUNK -eq 1 ] && filename="${name[${idx}]}_chunk_pr_${base_bound[$mem]}" || filename="${name[${idx}]}_mmap_pr_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make Components
        for mem in {0,1,2,3};
        do
            clear_pagecaches
            [ $USE_CHUNK -eq 1 ] && commandargs="./Components -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}" || commandargs="./Components -b ${data[${idx}]}"
            [ $USE_CHUNK -eq 1 ] && filename="${name[${idx}]}_chunk_cc_${base_bound[$mem]}" || filename="${name[${idx}]}_mmap_cc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done
        
        make KCore
        for mem in {0,1,2,3};
        do
            clear_pagecaches
            [ $USE_CHUNK -eq 1 ] && commandargs="./KCore -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}" || commandargs="./KCore -b ${data[${idx}]}"
            [ $USE_CHUNK -eq 1 ] && filename="${name[${idx}]}_chunk_kc_${base_bound[$mem]}" || filename="${name[${idx}]}_mmap_kc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make Radii
        for mem in {0,1,2,3};
        do
            clear_pagecaches
            [ $USE_CHUNK -eq 1 ] && commandargs="./Radii -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}" || commandargs="./Radii -b ${data[${idx}]}"
            [ $USE_CHUNK -eq 1 ] && filename="${name[${idx}]}_chunk_radii_${base_bound[$mem]}" || filename="${name[${idx}]}_mmap_radii_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done
    done
fi