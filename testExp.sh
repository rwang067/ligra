#!/bin/bash

USE_CHUNK=1
# 0: ligra-mmap; 1: ligra-chunk

[ $USE_CHUNK -eq 1 ] && export CHUNK=1

[ $USE_CHUNK -eq 1 ] && DATA_PATH=/mnt/nvme1/zorax/chunks/ || DATA_PATH=/mnt/nvme1/zorax/datasets/
CGROUP_PATH=/sys/fs/cgroup/memory/chunkgraph/
# CPU:use NUMA 0 node, with id 0-23 and 48-71, with taskset command
# TEST_CPU_SET="taskset --cpu-list 0-95:1"
TEST_CPU_SET="taskset -c 0-23,48-71:1"

PERF_CACHE_MISS=0
PERF_PAGECACHE=0
[ $PERF_CACHE_MISS -eq 1 ] && export CACHEMISS=1
[ $PERF_PAGECACHE -eq 1 ] && export PAGECACHE=1

export OMP_PROC_BIND=true

name[0]=twitter
name[1]=friendster
name[2]=ukdomain
name[3]=kron28
name[4]=kron29
name[5]=kron30
name[6]=yahoo

debug=false
chunkgraph=false
ligra_mmap=false
multithread=false
threshold=false
multilevel=true

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
declare -a kcore_iter=(10 10 10 10 10 3 10)

function set_schedule {
	SCHEDULE=$1
	export OMP_SCHEDULE="${SCHEDULE}"
}

set_schedule "dynamic"

clear_pagecaches() { 
    echo "zwx.1005" | sudo -S sysctl -w vm.drop_caches=3;
}

allocate_hugepage() {
    pagenum=$1
    echo "allocate hugepage: " $pagenum
    echo "zwx.1005" | sudo -S sudo sysctl -w vm.nr_hugepages=$pagenum;
}

clear_hugepage() { 
    echo "zwx.1005" | sudo -S sudo sysctl -w vm.nr_hugepages=0;
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
    if [ $PERF_CACHE_MISS -eq 1 ]; then
        cat ${log_dir}/${filename}.cachemiss >> ${log_dir}/${filename}.txt
    fi

    if $not_save_detail_log; then
        if [ $PERF_CACHE_MISS -eq 1 ]; then
            rm -f ${log_dir}/${filename}.cachemiss
        fi
        rm -f ${log_dir}/${filename}.diskio ${log_dir}/${filename}.iostat
    fi
}

if $debug; then
    log_time=$(date "+%Y%m%d_%H%M%S")
    mkdir -p results/logs/${log_time}
    cd apps && make clean

    outputFile="../results/hierg_query_time.csv"
    title="ChunkGraph"
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test ${title} Query Performace" >> ${outputFile}

    bounds=('256'
        '256'
        '256'
        '256'
        '256'
        '256'
        '256')

    for idx in 0;
    do
        len=1

        make PageRankDelta
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./PageRankDelta -b -chunk -threshold 20 ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_pr"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done

        # make PageRankDelta
        # for ((mem=0;mem<$len;mem++))
        # do
        #     clear_pagecaches
        #     commandargs="./PageRankDelta -b -chunk -threshold 20 -reorder ${data[${idx}]}"
        #     filename="${name[${idx}]}_chunk_reorder_pr"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done
    done

fi

if $chunkgraph; then
    log_time=$(date "+%Y%m%d_%H%M%S")
    mkdir -p results/logs/${log_time}
    cd apps && make clean

    outputFile="../results/hierg_query_time.csv"
    title="ChunkGraph"
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test ${title} Query Performace" >> ${outputFile}

    # roots:        TT   FS     UK     K29       YW       K30
    declare -a rts=(12 801109 5699262  310059974 35005211 233665123)

    data[0]=/mnt/nvme1/zorax/debug/twitter/twitter
    data[1]=/mnt/nvme1/zorax/debug/friendster/friendster
    data[2]=/mnt/nvme1/zorax/debug/ukdomain/ukdomain
    data[3]=/mnt/nvme1/zorax/debug/kron29/kron29
    data[4]=/mnt/nvme1/zorax/debug/yahoo/yahoo
    data[5]=/mnt/nvme1/zorax/debug/kron30/kron30

    name[0]=twitter
    name[1]=friendster
    name[2]=ukdomain
    name[3]=kron29
    name[4]=yahoo
    name[5]=kron30

    bounds=('256'
        '256'
        '256'
        '256'
        '256'
        '256')

    declare -a kcore_iter=(10 10 10 10 3 10)

    for idx in {0,1,2,3,4,5};
    # for idx in 4;
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
            commandargs="./BFS -b -r ${rts[$idx]} -chunk -threshold 5 -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bfs"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done

        make BC
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./BC -b -r ${rts[$idx]} -chunk -threshold 5 -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bc"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done

        make PageRankDelta
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./PageRankDelta -b -chunk -onlyout -threshold 5 -buffer ${base_bound[$mem]} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_prd"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done

        # make Components
        # for ((mem=0;mem<$len;mem++))
        # do
        #     clear_pagecaches
        #     commandargs="./Components -b -chunk -threshold 10 -buffer ${base_bound[$mem]} ${data[${idx}]}"
        #     filename="${name[${idx}]}_chunk_cc"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done
        
        # make KCore
        # for ((mem=0;mem<$len;mem++))
        # do
        #     clear_pagecaches
        #     commandargs="./KCore -b -maxk ${kcore_iter[$idx]} -chunk -threshold 20 -buffer ${base_bound[$mem]} ${data[${idx}]}"
        #     filename="${name[${idx}]}_chunk_kc"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done

        # make Radii
        # for ((mem=0;mem<$len;mem++))
        # do
        #     clear_pagecaches
        #     commandargs="./Radii -b -chunk -threshold 5 -buffer ${base_bound[$mem]} ${data[${idx}]} "
        #     filename="${name[${idx}]}_chunk_radii"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done

        # make MIS
        # for ((mem=0;mem<$len;mem++))
        # do
        #     clear_pagecaches
        #     commandargs="./MIS -b -chunk -threshold 10 -buffer ${base_bound[$mem]} ${data[${idx}]} "
        #     filename="${name[${idx}]}_chunk_mis"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done

        make BellmanFord
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./BellmanFord -b  -r ${rts[$idx]} -chunk -threshold 5 -buffer ${base_bound[$mem]} ${data[${idx}]} "
            filename="${name[${idx}]}_chunk_bf"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done
    done
fi

if $multithread; then
    # declare -a thread_list=(2 4 8 16 32 48)
    # declare -a thread_list=(8 16 32 48 64 80 96)
    declare -a thread_list=(2 4 8 16 24 32 40 48)

    log_time=$(date "+%Y%m%d_%H%M%S")
    mkdir -p results/logs/${log_time}
    cd apps && make clean
    
    outputFile="../results/hierg_query_time.csv"
    title="ChunkGraph-multithread"
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test ${title} Query Performace" >> ${outputFile}

    # roots:        TT   FS     UK     K29       YW       K30
    declare -a rts=(12 801109 5699262  310059974 35005211 233665123)

    data[0]=/mnt/nvme1/zorax/debug/twitter/twitter
    data[1]=/mnt/nvme1/zorax/debug/friendster/friendster
    data[2]=/mnt/nvme1/zorax/debug/ukdomain/ukdomain
    data[3]=/mnt/nvme1/zorax/debug/kron29/kron29
    data[4]=/mnt/nvme1/zorax/debug/yahoo/yahoo
    data[5]=/mnt/nvme1/zorax/debug/kron30/kron30

    name[0]=twitter
    name[1]=friendster
    name[2]=ukdomain
    name[3]=kron29
    name[4]=yahoo
    name[5]=kron30

    bounds=('256'
        '256'
        '256'
        '256'
        '256'
        '256')

    declare -a kcore_iter=(10 10 10 10 3 10)

    # for idx in {0,1,2,3,4,5};
    # for idx in {1,4,5};
    for idx in 5;
    do
        echo -n "Data: "
        echo ${data[$idx]}
        echo -n "Root: "
        echo ${rts[$idx]}
        
        make BFS
        for thread in ${thread_list[@]};
        do
            clear_pagecaches
            commandargs="./BFS -b -r ${rts[$idx]} -chunk -threshold 5 -buffer 256 -t ${thread} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bfs_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done
        
        make BC
        for thread in ${thread_list[@]};
        do
            clear_pagecaches
            commandargs="./BC -b -r ${rts[$idx]} -chunk -threshold 5 -buffer 256 -t ${thread} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bc_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done

        continue

        make PageRankDelta
        for thread in ${thread_list[@]};
        do
            clear_pagecaches
            commandargs="./PageRankDelta -b -chunk -onlyout -threshold 5 -buffer 256 -t ${thread} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_prd_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done

        # make Components
        # for thread in ${thread_list[@]};
        # do    
        #     clear_pagecaches
        #     commandargs="./Components -b -chunk -threshold 10 -buffer 256 -t ${thread} ${data[${idx}]}"
        #     filename="${name[${idx}]}_chunk_cc_${thread}t"
        
        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done  

        make KCore
        for thread in ${thread_list[@]};
        do
            clear_pagecaches
            commandargs="./KCore -b -maxk ${kcore_iter[$idx]} -chunk -threshold 5 -buffer 256 -t ${thread} ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_kc_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done

        continue

        make Radii
        for thread in ${thread_list[@]};
        do
            clear_pagecaches
            commandargs="./Radii -b -chunk -threshold 5 -buffer 256 -t ${thread} ${data[${idx}]} "
            filename="${name[${idx}]}_chunk_radii_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done

        # make MIS
        # for thread in ${thread_list[@]};
        # do
        #     clear_pagecaches
        #     commandargs="./MIS -b -chunk -threshold 10 -buffer 256 -t ${thread} ${data[${idx}]} "
        #     filename="${name[${idx}]}_chunk_mis_${thread}t"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done

        make BellmanFord
        for thread in ${thread_list[@]};
        do
            clear_pagecaches
            commandargs="./BellmanFord -b  -r ${rts[$idx]} -chunk -threshold 5 -buffer 256 -t ${thread} ${data[${idx}]} "
            filename="${name[${idx}]}_chunk_bf_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done
    done
fi


if $threshold; then
    log_time=$(date "+%Y%m%d_%H%M%S")
    mkdir -p results/logs/${log_time}
    cd apps && make clean
    
    outputFile="../results/hierg_query_time.csv"
    title="ChunkGraph-multithread"
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test ${title} Query Performace" >> ${outputFile}

    data[0]=/mnt/nvme1/zorax/debug/yahoo/yahoo
    data[1]=/mnt/nvme1/zorax/debug/yahoo_25/yahoo
    data[2]=/mnt/nvme1/zorax/debug/yahoo_50/yahoo
    data[3]=/mnt/nvme1/zorax/debug/yahoo_75/yahoo
    data[4]=/mnt/nvme1/zorax/chunks/yahoo/yahoo

    name[0]=yahoo
    name[1]=yahoo_25
    name[2]=yahoo_50
    name[3]=yahoo_75
    name[4]=yahoo_100

    bounds=('256'
        '256'
        '256'
        '256'
        '256'
        '256')

    declare -a kcore_iter=(3 3 3 3 3 3)
    declare -a rts=(35005211 35005211 35005211 35005211 35005211 35005211)

    len=1

    # for idx in {0,1,2,3,4};
    # for idx in {1,4,5};
    for idx in 3;
    do
        echo -n "Data: "
        echo ${data[$idx]}
        echo -n "Root: "
        echo ${rts[$idx]}
        
        make BFS
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./BFS -b -r ${rts[$idx]} -chunk -threshold 5 -buffer 256 ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bfs_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done
        
        make BC
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            commandargs="./BC -b -r ${rts[$idx]} -chunk -threshold 5 -buffer 256 ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bc_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
        done

        continue

        # make PageRankDelta
        # for ((mem=0;mem<$len;mem++))
        # do
        #     clear_pagecaches
        #     commandargs="./PageRankDelta -b -chunk -onlyout -threshold 5 -buffer 256 ${data[${idx}]}"
        #     filename="${name[${idx}]}_chunk_prd_${thread}t"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done

        # make Components
        # for ((mem=0;mem<$len;mem++))
        # do    
        #     clear_pagecaches
        #     commandargs="./Components -b -chunk -threshold 10 -buffer 256 ${data[${idx}]}"
        #     filename="${name[${idx}]}_chunk_cc_${thread}t"
        
        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done  

        # make KCore
        # for ((mem=0;mem<$len;mem++))
        # do
        #     clear_pagecaches
        #     commandargs="./KCore -b -maxk ${kcore_iter[$idx]} -chunk -threshold 5 -buffer 256 ${data[${idx}]}"
        #     filename="${name[${idx}]}_chunk_kc_${thread}t"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done

        # make Radii
        # for ((mem=0;mem<$len;mem++))
        # do
        #     clear_pagecaches
        #     commandargs="./Radii -b -chunk -threshold 5 -buffer 256 ${data[${idx}]} "
        #     filename="${name[${idx}]}_chunk_radii_${thread}t"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done

        # make MIS
        # for ((mem=0;mem<$len;mem++))
        # do
        #     clear_pagecaches
        #     commandargs="./MIS -b -chunk -threshold 10 -buffer 256 ${data[${idx}]} "
        #     filename="${name[${idx}]}_chunk_mis_${thread}t"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done

        # make BellmanFord
        # for ((mem=0;mem<$len;mem++))
        # do
        #     clear_pagecaches
        #     commandargs="./BellmanFord -b  -r ${rts[$idx]} -chunk -threshold 5 -buffer 256 ${data[${idx}]} "
        #     filename="${name[${idx}]}_chunk_bf_${thread}t"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done
    done
fi


if $multilevel; then
    log_time=$(date "+%Y%m%d_%H%M%S")
    mkdir -p results/logs/${log_time}
    cd apps && make clean
    
    outputFile="../results/hierg_query_time.csv"
    title="ChunkGraph-multilevel"
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test ${title} Query Performace" >> ${outputFile}

    # data[0]=/mnt/nvme1/zorax/multi/twitter_0/twitter
    data[0]=/mnt/nvme1/zorax/debug/kron30/kron30
    data[1]=/mnt/nvme1/zorax/multi/kron30_l3_10/kron30
    data[2]=/mnt/nvme1/zorax/multi/kron30_l5_10/kron30
    data[3]=/mnt/nvme1/zorax/debug/yahoo/yahoo
    data[4]=/mnt/nvme1/zorax/multi/yahoo_l3_10/yahoo
    data[5]=/mnt/nvme1/zorax/multi/yahoo_l5_10/yahoo
    data[6]=/mnt/nvme1/zorax/multi/twitter_l5_10/twitter

    name[0]=kron30
    name[1]=kron30_l3_10
    name[2]=kron30_l5_10
    name[3]=yahoo
    name[4]=yahoo_l3_10
    name[5]=yahoo_l5_10
    name[6]=twitter_l5_10

    bounds=('256'
        '256'
        '256'
        '256'
        '256'
        '256'
        '256')

    declare -a kcore_iter=(10 10 10 3 3 3 10)
    declare -a rts=(233665123 233665123 233665123 35005211 35005211 35005211 12)

    declare -a hugepage=(0 35721 5817 0 7961 0 178)

    len=1

    # for idx in {0,1,2,3,4};
    # for idx in {1,2,3,4}
    # for idx in {0,1,3,4}
    for idx in {0,1,2};
    do
        echo -n "Data: "
        echo ${data[$idx]}
        echo -n "Root: "
        echo ${rts[$idx]}
        
        make BFS
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            # allocate_hugepage ${hugepage[$idx]}
            commandargs="./BFS -b -r ${rts[$idx]} -chunk -threshold 5 -buffer 256 ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bfs_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
            clear_hugepage
        done
        
        make BC
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            # allocate_hugepage ${hugepage[$idx]}
            commandargs="./BC -b -r ${rts[$idx]} -chunk -threshold 5 -buffer 256 ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_bc_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
            clear_hugepage
        done

        make PageRankDelta
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            # allocate_hugepage ${hugepage[$idx]}
            commandargs="./PageRankDelta -b -chunk -onlyout -threshold 5 -buffer 256 ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_prd_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
            clear_hugepage
        done

        # make Components
        # for ((mem=0;mem<$len;mem++))
        # do    
        #     clear_pagecaches
        #     # allocate_hugepage ${hugepage[$idx]}
        #     commandargs="./Components -b -chunk -threshold 10 -buffer 256 ${data[${idx}]}"
        #     filename="${name[${idx}]}_chunk_cc_${thread}t"
        
        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        #     clear_hugepage
        # done  

        make KCore
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            # allocate_hugepage ${hugepage[$idx]}
            commandargs="./KCore -b -maxk ${kcore_iter[$idx]} -chunk -threshold 5 -buffer 256 ${data[${idx}]}"
            filename="${name[${idx}]}_chunk_kc_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
            clear_hugepage
        done

        make Radii
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            # allocate_hugepage ${hugepage[$idx]}
            commandargs="./Radii -b -chunk -threshold 5 -buffer 256 ${data[${idx}]} "
            filename="${name[${idx}]}_chunk_radii_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
            clear_hugepage
        done

        # make MIS
        # for ((mem=0;mem<$len;mem++))
        # do
        #     clear_pagecaches
        #     commandargs="./MIS -b -chunk -threshold 10 -buffer 256 ${data[${idx}]} "
        #     filename="${name[${idx}]}_chunk_mis_${thread}t"

        #     profile_performance "\${commandargs}" "\${filename}"
        #     wait
        # done

        make BellmanFord
        for ((mem=0;mem<$len;mem++))
        do
            clear_pagecaches
            # allocate_hugepage ${hugepage[$idx]}
            commandargs="./BellmanFord -b  -r ${rts[$idx]} -chunk -threshold 5 -buffer 256 ${data[${idx}]} "
            filename="${name[${idx}]}_chunk_bf_${thread}t"

            profile_performance "\${commandargs}" "\${filename}"
            wait
            clear_hugepage
        done
    done
fi