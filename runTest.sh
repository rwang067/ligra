#!/bin/bash

DATA_PATH=/mnt/nvme1/zorax/datasets/csr_bin/
CGROUP_PATH=/sys/fs/cgroup/memory/chunkgraph/
TEST_CPU_SET="taskset --cpu-list 49-96:1"

name[0]=twitter
name[1]=friendster
name[2]=ukdomain
name[3]=kron28
name[4]=kron29
name[5]=kron30
name[6]=yahoo

TEST=/home/cxy/ligra-mmap/inputs/rMatGraph_J_5_100
data[0]=${DATA_PATH}Twitter/twitter
data[1]=${DATA_PATH}Friendster/friendster
data[2]=${DATA_PATH}Ukdomain/ukdomain
data[3]=${DATA_PATH}Kron28/kron28
data[4]=${DATA_PATH}Kron29/kron29
data[5]=${DATA_PATH}Kron30/kron30
data[6]=${DATA_PATH}Yahoo/yahoo

# roots:        TT       FS       UK      K28       K29       K30       YW       K31       CW
# declare -a rts=(18225348 26737282 5699262 254655025 310059974 233665123 35005211 691502068 739935047)
declare -a rts=(12 801109 5699262 254655025 310059974 233665123 35005211 691502068 739935047)
declare -a base_bound=(8 12 16 128)

#mkdir -p results_small
export OMP_PROC_BIND=true

function set_schedule {
	SCHEDULE=$1
	export OMP_SCHEDULE="${SCHEDULE}"
}

set_schedule "dynamic"

# cd apps && make TestAll
# cd apps && make clean && make BFS PageRank Components KCore
# cd apps && make clean && make testNebrs BFS BC PageRank

clear_pagecaches() { 
    echo "zwx.1005" | sudo -S sysctl -w vm.drop_caches=3;
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

    # nohup /home/cxy/snoop-ligra/iosnoop.sh -p $pid > ../results/${filename}.snoop &
    # perf record -e major-faults, -p $pid -o ../result/${filename}.perf &
    # perf record -e major-faults,cache-misses -p $pid -o ../results/${filename}.perf &
    wait $pid

    res=$(awk 'NR>5 {sum+=$5} END {print sum}' ${log_dir}/${filename}.diskio)
    echo "total bytes read during compute: " $res "KB ("$(echo "scale=2; $res/1024/1024" | bc) "GB)" >> ${log_dir}/${filename}.txt
    echo "total bytes read during compute: " $res "KB ("$(echo "scale=2; $res/1024/1024" | bc) "GB)" >> ${log_dir}/${filename}.diskio

    # snoop_pid=$(ps -ef | grep trace_pipe | grep -v grep | awk '{print $2}')
    # echo $snoop_pid
    # kill -9 $snoop_pid
}

cgroup_swap=true
debug=false

if $cgroup_swap; then
    # cd apps && make clean && make testNebrs BFS BC PageRank Components KCore Radii
    # cd apps && make clean && make BFS
    cd apps && make clean
    
    memory_bound[0]=$((${base_bound[0]}*1024*1024*1024))
    memory_bound[1]=$((${base_bound[1]}*1024*1024*1024))
    memory_bound[2]=$((${base_bound[2]}*1024*1024*1024))
    memory_bound[3]=$((${base_bound[3]}*1024*1024*1024))

    outputFile="../results/ligra_swap_query_time.csv"
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test Ligra-swap Query Performace" >> ${outputFile}
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
            commandargs="./BFS -b -r ${rts[$idx]} ${data[${idx}]}"
            filename="${name[${idx}]}_ligra_swap_bfs_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            # echo ${memsw_bound[$idx]} > ${CGROUP_PATH}/memory.memsw.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        exit
        
        make BC
        for mem in {0,1,2,3};
        # for mem in 3;
        do
            clear_pagecaches
            commandargs="./BC -b -r ${rts[$idx]} ${data[${idx}]}"
            filename="${name[${idx}]}_ligra_swap_bc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            # echo ${memsw_bound[$idx]} > ${CGROUP_PATH}/memory.memsw.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make PageRank
        for mem in {0,1,2,3};
        do
            clear_pagecaches
            commandargs="./PageRank -b ${data[${idx}]}"
            filename="${name[${idx}]}_ligra_swap_pr_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            # echo ${memsw_bound[$idx]} > ${CGROUP_PATH}/memory.memsw.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make Components
        for mem in {0,1,2,3};
        # for mem in 3;
        do
            clear_pagecaches
            commandargs="./Components -b ${data[${idx}]}"
            filename="${name[${idx}]}_ligra_swap_cc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            # echo ${memsw_bound[$idx]} > ${CGROUP_PATH}/memory.memsw.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make KCore
        for mem in {0,1,2,3};
        # for mem in 3;
        do
            clear_pagecaches
            commandargs="./KCore -b ${data[${idx}]}"
            filename="${name[${idx}]}_ligra_swap_kc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            # echo ${memsw_bound[$idx]} > ${CGROUP_PATH}/memory.memsw.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make Radii
        for mem in {0,1,2,3};
        # for mem in 3;
        do
            clear_pagecaches
            commandargs="./Radii -b ${data[${idx}]}"
            filename="${name[${idx}]}_ligra_swap_radii_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            # echo ${memsw_bound[$idx]} > ${CGROUP_PATH}/memory.memsw.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done
    done
fi

if $debug; then
    cd apps && make clean && make BFS
    ./BFS -b -r ${rts[0]} ${data[0]}
    # valgrind --tool=massif --massif-out-file=massif.out.%p ./BFS -b -r ${rts[$idx]} ${data[${idx}]}
    # ./Components -b ${data[${idx}]}
    # ./PageRank -b ${data[${idx}]}
fi