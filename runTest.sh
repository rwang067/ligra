#!/bin/bash

export OMP_PROC_BIND=true

function set_schedule {
	SCHEDULE=$1
	export OMP_SCHEDULE="${SCHEDULE}"
}

#mkdir -p results_small

set_schedule "dynamic,64"

# cd apps && make TestAll
# cd apps && make clean && make BFS PageRank Components KCore

# roots:        TT       FS       UK      K28       K29       K30       YW       K31       CW
declare -a rts=(18225348 26737282 5699262 254655025 310059974 233665123 35005211 691502068 739935047)

profile_run() {
    eval commandargs="$1"
    eval filename="$2"

    echo $commandargs
    echo $filename
    
    commandname=$(echo $commandargs | awk '{print $1}')

    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "profile run with command: " $commandargs > ../${filename}.txt

    nohup $commandargs >> ../${filename}.txt &
    pid=$(ps -ef | grep $commandname | grep -v grep | awk '{print $2}')

    iostat | sed '7,32d' | head -n 7 > ../${filename}_iostat.txt

    pidstat -p $pid -dl 1 > ../${filename}_disk.txt &
    pidstat -p $pid -rl 1 > ../${filename}_mem.txt &
    wait $pid
    iostat | sed '7,32d' | head -n 7 >> ../${filename}_iostat.txt

    # calculate total disk
    rdisk=$(cat ../${filename}_disk.txt | sed '1,3d' | awk 'BEGIN {sum=0} {sum += $5} END {print sum}')
    echo 'total disk read: '${rdisk} >> ../${filename}_disk.txt

    # calculate total memory and page faults
    min_flt=$(cat ../${filename}_mem.txt | sed '1,3d' | awk 'BEGIN {sum=0} {sum += $5} END {print sum}')
    maj_flt=$(cat ../${filename}_mem.txt | sed '1,3d' | awk 'BEGIN {sum=0} {sum += $6} END {print sum}')
    echo 'total minor fault: '${min_flt} >> ../${filename}_mem.txt
    echo 'total major fault: '${maj_flt} >> ../${filename}_mem.txt
}

TEST_ALL=false
DEBUG=true

TEST=/home/cxy/ligra-mmap/inputs/rMatGraph_J_5_100
data[0]=/data1/zorax/datasets/Twitter/row_0_col_0/out.txt
data[1]=/data1/wr/datasets/Friendster/csr_bin/out.friendster
data[2]=/data1/wr/datasets/Ukdomain/csr_bin/out.uk
data[3]=/data1/wr/datasets/Kron28/csr_bin/out
data[4]=/data1/wr/datasets/Kron29/csr_bin/out
data[5]=/data1/wr/datasets/Kron30/csr_bin/out
data[6]=/data1/wr/datasets/Yahoo/csr_bin/out
data[7]=/data2/graph/Kron31/csr_bin/out
data[8]=/data1/wr/datasets/Crawl/csr_bin/crawl

name[0]=twitter
name[1]=friendster
name[2]=ukdomain
name[3]=kron28
name[4]=kron29
name[5]=kron30
name[6]=yahoo

# echo $$ | sudo tee /sys/fs/cgroup/cxy-test/cgroup.procs

if $TEST_ALL; then
    # for idx in {0,1,2,3,4,5,6}
    cd apps
    for idx in {4,5,6}
    do
        echo -n "Data: "
        echo ${data[$idx]}
        echo -n "Root: "
        echo ${rts[$idx]}
        echo "---------swap version-----------"
        # sysctl -w vm.drop_caches=3
        commandargs="./BFS -b -r ${rts[$idx]} ${data[${idx}]}"
        filename="${name[${idx}]}_swap_bfs"
        profile_run "\${commandargs}" "\${filename}"
        wait
        # sysctl -w vm.drop_caches=3
        commandargs="./Components -b ${data[${idx}]}"
        filename="${name[${idx}]}_swap_cc"
        profile_run "\${commandargs}" "\${filename}"
        wait
        # sysctl -w vm.drop_caches=3
        commandargs="./KCore -b ${data[${idx}]}"
        filename="${name[${idx}]}_swap_kc"
        profile_run "\${commandargs}" "\${filename}"
        wait
        # sysctl -w vm.drop_caches=3
        commandargs="./PageRank -b ${data[${idx}]}"
        filename="${name[${idx}]}_swap_pr"
        profile_run "\${commandargs}" "\${filename}"
        wait
    done
fi

if $DEBUG; then
    cd apps && make BFS PageRank Components KCore

    ./apps/PageRank -b ${data[4]}
    commandargs="./BFS -b -r ${rts[6]} ${data[6]}"
    filename="${name[6]}_swap_bfs"
    profile_run "\${commandargs}" "\${filename}"

fi