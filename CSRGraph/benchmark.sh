#!/bin/bash

DATA_PATH=/mnt/nvme1/zorax/datasets/csr_bin/
SSD_PATH=/mnt/nvme2/zorax/testCSRGraph/
CGROUP_PATH=/sys/fs/cgroup/memory/chunkgraph/

mkdir -p ${SSD_PATH}

clear_pagecaches() { 
    echo "zwx.1005" | sudo -S sysctl -w vm.drop_caches=3;
}

clear_ssd() {
    rm -rf ${SSD_PATH}/*
}

make

# roots:        TT FS       UK      K28       K29       K30       YW       K31       CW
declare -a rts=(12 801109 5699262 254655025 310059974 233665123 35005211 691502068 739935047)
# declare -a rts=(18225348 26737282 5699262 254655025 310059974 233665123 35005211 691502068 739935047)

profile_run() {
    eval commandargs="$1"
    eval filename="$2"

    echo "command: "$commandargs >> command.log
    echo "data: "$filename >> command.log

    commandname=$(echo $commandargs | awk '{print $1}')

    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "profile run with command: " $commandargs > ${filename}

    nohup $commandargs >> ${filename} &
    pid=$(ps -ef | grep $commandname | grep -v grep | awk '{print $2}')
    echo "pid:" $pid >> command.log

    sudo perf record -e cache-misses -p $pid -o ${filename}.perf &

    echo $pid > ${CGROUP_PATH}/cgroup.procs
    echo "cgroup tasks:" ${CGROUP_PATH}tasks >> command.log

    wait $pid

    # nohup /home/cxy/snoop-ligra/iosnoop.sh -p $pid > ../result/${filename}.snoop &
    # perf record -e major-faults -p $pid -o ../result/${filename}.perf &
    # wait $pid

    # snoop_pid=$(ps -ef | grep trace_pipe | grep -v grep | awk '{print $2}')
    # echo $snoop_pid
    # kill -9 $snoop_pid
}

data[0]=${DATA_PATH}Twitter/
data[1]=${DATA_PATH}Friendster/
data[2]=${DATA_PATH}Ukdomain/
data[3]=${DATA_PATH}Kron28/
data[4]=${DATA_PATH}Kron29/
data[5]=${DATA_PATH}Kron30/
data[6]=${DATA_PATH}Yahoo/

name[0]=twitter
name[1]=friendster
name[2]=ukdomain
name[3]=kron28
name[4]=kron29
name[5]=kron30
name[6]=yahoo

swap=false
cgroup_limit=false
debug=false
convert_chunk=false
count_degree=false
convert_blaze=true

if $convert_chunk; then
    declare -a sblk_size=(128 128 256 512 768 768 768)
    SAVE_PATH=/mnt/nvme1/zorax/chunks/
    # for idx in {0,1,2,3,4,5,6};
    # for idx in {1,2,3,4};
    for idx in 6;
    do
        for job in 6;
        do
            mkdir -p ${SSD_PATH}
            mkdir -p ${SAVE_PATH}${name[${idx}]}

            nverts=$(cat ${data[${idx}]}/${name[${idx}]}.config)
            echo $nverts
            clear_ssd
            # gdb --args 
            ./bin/main -f ${data[${idx}]} --prefix ${name[${idx}]} --ssd ${SSD_PATH} --source ${rts[${idx}]} --sblk_pool_size ${sblk_size[${idx}]} -t 1 -q 0 -j ${job} -v ${nverts} &> ${name[${idx}]}_convert.out
            mv ${SSD_PATH}/* ${SAVE_PATH}${name[${idx}]}
        done
    done
fi

if $convert_blaze; then
    SAVE_PATH=/mnt/nvme2/blaze/
    mkdir -p ${SAVE_PATH}

    for idx in 0;
    do
        job=1
        mkdir -p ${SSD_PATH}
        mkdir -p ${SAVE_PATH}${name[${idx}]}
        nverts=$(cat ${data[${idx}]}/${name[${idx}]}.config)
        echo $nverts
        clear_ssd
        # gdb --args
        ./bin/main -f ${data[${idx}]} --prefix ${name[${idx}]} --ssd ${SSD_PATH} -t 48 -q 0 -j ${job} -v ${nverts} &> ${name[${idx}]}_convert_blaze.out
        mv ${SSD_PATH}/* ${SAVE_PATH}${name[${idx}]}
    done
fi

if $debug; then
    # gdb --args ./bin/main -f ${data[0]} --prefix ${name[0]} --ssd ${SSD_PATH} --source ${rts[0]} -q 1 -t 1
    echo "==========================" >> result.txt
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test Query Performace" >> result.txt
    # clear_pagecaches
    # ./bin/main -f ${data[0]} --prefix ${name[0]} --ssd ${SSD_PATH} --source ${rts[0]} -q 1 -t 16 -j 1 > test_csrgraph.out
    # clear_pagecaches
    # ./bin/main -f ${data[0]} --prefix ${name[0]} --ssd ${SSD_PATH} --source ${rts[0]} -q 1 -t 1 -j 2 >> test_csrchunkgraph.out
    # clear_pagecaches
    # ./bin/main -f ${data[0]} --prefix ${name[0]} --ssd ${SSD_PATH} --source ${rts[0]} -q 1 -t 1 -j 3 >> test_inplacegraph.out
    # clear_pagecaches
    # ./bin/main -f ${data[0]} --prefix ${name[0]} --ssd ${SSD_PATH} --source ${rts[0]} -q 1 -j 4 > test_regraph.out

    clear_pagecaches
    clear_ssd
    gdb --args ./bin/main -f ${data[2]} --prefix ${name[2]} --ssd ${SSD_PATH} --source ${rts[2]} -q 1 -j 5

    # clear_pagecaches
    # gdb --args ./bin/main -f ${data[0]} --prefix ${name[0]} --ssd ${SSD_PATH} --source ${rts[0]} -q 1 -j 2

    # ./bin/main -f ${DATA_PATH}/Twitter/ --prefix out.txt --ssd ${SSD_PATH} --source 18225348 -q 5 -t 16 > twitter.out
    # ./bin/main -f ${DATA_PATH}/Friendster/ --prefix out.friendster --ssd ${SSD_PATH} --source 18225348 -q 5 -t 16 > friendster.out
fi

if $cgroup_limit; then
    memory_bound[0]=$((32*1024*1024*1024))  # 16GB
    memory_bound[1]=$((32*1024*1024*1024))  # 16GB
    memory_bound[2]=$((32*1024*1024*1024))  # 16GB
    memory_bound[3]=$((48*1024*1024*1024))  # 48GB
    memory_bound[4]=$((48*1024*1024*1024))  # 48GB
    memory_bound[5]=$((64*1024*1024*1024))  # 64GB
    memory_bound[6]=$((64*1024*1024*1024))  # 64GB
    # for idx in {0,1,2,3,4,5,6};
    for idx in 0;
    do
        echo "==========================" >> command.log
        echo "==========================" >> result.txt
        cur_time=$(date "+%Y-%m-%d %H:%M:%S")
        echo $cur_time "Test Query Performace" >> command.log
        echo $cur_time "Test Query Performace" >> result.txt
        echo "data: ${data[$idx]}" >> command.log
        echo "root: ${rts[$idx]}" >> command.log
        echo "memory bound: ${memory_bound[$idx]}" >> command.log

        echo ${memory_bound[$idx]} > ${CGROUP_PATH}/memory.limit_in_bytes

        thread_nums=(1 1 1 1 1)
        # for job in {1,2,3,4,5}
        for job in 5;
        do
            echo "Test Job ${job}" >> command.log
            clear_pagecaches
            clear_ssd
            commandargs="./bin/main -f ${data[${idx}]} --prefix ${name[${idx}]} --ssd ${SSD_PATH} --source ${rts[${idx}]} -t ${thread_nums[${idx}]} -q 1 -j ${job}"
            filename="${name[${idx}]}_testcase${job}.out"
            profile_run "\${commandargs}" "\${filename}"
        done

        echo "==========================" >> command.log
    done
fi

if $swap; then
    outputFile=test_csrchunk_graph.out
    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "Test Query Performace" >> ${outputFile}
    for i in {0,1}
    do
        echo "==========================" >> ${outputFile}
        echo "data: ${data[$i]}" >> ${outputFile}
        echo "root: ${rts[$i]}" >> ${outputFile}
        echo "==========================" >> ${outputFile}
        ./bin/main -f ${data[$i]} --prefix ${name[$i]} --ssd ${SSD_PATH} --source ${rts[$i]} -q 1 -t 16 >> ${outputFile}
    done
fi

if $count_degree; then
    for i in {0,1,2,3,4,5,6};
    do
        outputFile=count_degree_${name[$i]}.out
        ./bin/main -j 1 -f ${data[$i]} --prefix ${name[$i]} --ssd ${SSD_PATH} -q 0 > ${outputFile}
    done
fi