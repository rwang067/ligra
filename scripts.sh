#!/bin/bash

DATA_PATH=/mnt/nvme1/zorax/chunks/
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

data[0]=${DATA_PATH}twitter/${name[0]}
data[1]=${DATA_PATH}friendster/${name[1]}
data[2]=${DATA_PATH}csr_bin/Ukdomain/ukdomain
data[3]=${DATA_PATH}csr_bin/Kron28/kron28
data[4]=${DATA_PATH}csr_bin/Kron29/kron29
data[5]=${DATA_PATH}csr_bin/Kron30/kron30
data[6]=${DATA_PATH}csr_bin/Yahoo/yahoo

# roots:        TT       FS       UK      K28       K29       K30       YW       K31       CW
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

    # monitor and record the amount of physical memory used for the process pid

    echo "monitoring memory usage for pid:" $pid > ../results/logs/${log_time}/${filename}.memory
    echo "VmSize VmRSS VmSwap" >> ../results/logs/${log_time}/${filename}.memory
    maxVmPeak=0
    maxVmHWM=0
    while :
    do
        pid=$(ps -ef | grep $commandname | grep -v grep | awk '{print $2}')
        if [ -z "$pid" ]; then
            break
        fi
        cat /proc/$pid/status | grep -E "VmSize|VmRSS|VmSwap" | awk '{printf("%s ", $2)}' >> ../results/logs/${log_time}/${filename}.memory
        echo "" >> ../results/logs/${log_time}/${filename}.memory
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
    echo "maxVmPeak: " $maxVmPeak >> ../results/logs/${log_time}/${filename}.memory
    echo "maxVmHWM: " $maxVmHWM >> ../results/logs/${log_time}/${filename}.memory

    wait $pid
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

    outputFile="../results/hierg_query_time.csv"
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test ChunkGraph-swap Query Performace" >> ${outputFile}
    # for idx in {0,1};
    for idx in 1;
    do
        echo -n "Data: "
        echo ${data[$idx]}
        echo -n "Root: "
        echo ${rts[$idx]}

        # make BFS
        # for mem in {0,1,2,3};
        # # for mem in 3;
        # do
        #     clear_pagecaches
        #     commandargs="./BFS -b -r ${rts[$idx]} -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
        #     filename="${name[${idx}]}_chunk_bfs_${base_bound[$mem]}"
        #     echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
        #     # echo ${memsw_bound[$idx]} > ${CGROUP_PATH}/memory.memsw.limit_in_bytes

        #     profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
        #     wait
        # done

        make BC
        # for mem in {0,1,2,3};
        for mem in 3;
        do
            clear_pagecaches
            commandargs="./BC -b -r ${rts[$idx]} -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            # echo ${memsw_bound[$idx]} > ${CGROUP_PATH}/memory.memsw.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        exit

        make PageRank
        for mem in {0,1,2,3};
        do
            clear_pagecaches
            commandargs="./PageRank -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_pr_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes
            # echo ${memsw_bound[$idx]} > ${CGROUP_PATH}/memory.memsw.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make Components
        for mem in {0,1,2,3};
        do
            clear_pagecaches
            commandargs="./Components -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_cc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done
        
        make KCore
        for mem in {0,1,2,3};
        do
            clear_pagecaches
            commandargs="./KCore -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_kc_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done

        make Radii
        for mem in {0,1,2,3};
        do
            clear_pagecaches
            commandargs="./Radii -b -chunk -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_radii_${base_bound[$mem]}"
            echo ${memory_bound[$mem]} > ${CGROUP_PATH}/memory.limit_in_bytes

            profile_memory "\${commandargs}" "\${filename}" "\${base_bound[$mem]}"
            wait
        done
    done
fi