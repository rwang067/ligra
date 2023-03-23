#!/bin/bash

export OMP_PROC_BIND=true

function set_schedule {
	SCHEDULE=$1
	export OMP_SCHEDULE="${SCHEDULE}"
}

#mkdir -p results_small

set_schedule "dynamic,64"

# cd apps && make TestAll
# roots:        TT       FS       UK      K28       K29       K30       YW       K31       CW
declare -a rts=(18225348 26737282 5699262 254655025 310059974 233665123 35005211 691502068 739935047)

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

# echo $$ | sudo tee /sys/fs/cgroup/cxy-test/cgroup.procs

if $TEST_ALL; then
    for idx in 7
    do
        echo -n "Data: "
        echo ${data[$idx]}
        echo -n "Root: "
        echo ${rts[$idx]}
        echo "---------swap version-----------"
        sysctl -w vm.drop_caches=3
        ./apps/BFS -b -r ${rts[$idx]} ${data[$idx]}
        wait
        sysctl -w vm.drop_caches=3
        ./apps/PageRank -b ${data[$idx]}
        wait
        sysctl -w vm.drop_caches=3
        ./apps/Components -b ${data[$idx]}
        wait
        sysctl -w vm.drop_caches=3
        ./apps/KCore -b ${data[$idx]}
        wait
    done
fi

if $DEBUG; then
    cd apps && make BFS PageRank Components KCore && cd ..
    ./apps/BFS -b -r ${rts[4]} ${data[4]}
    ./apps/BFS -b -r ${rts[4]} ${data[4]}
    ./apps/BFS -b -r ${rts[4]} ${data[4]}

    ./apps/PageRank -b ${data[4]}
    ./apps/PageRank -b ${data[4]}
    ./apps/PageRank -b ${data[4]}

    ./apps/Components -b ${data[4]}
    ./apps/Components -b ${data[4]}
    ./apps/Components -b ${data[4]}
    
    ./apps/KCore -b ${data[4]}
    ./apps/KCore -b ${data[4]}
    ./apps/KCore -b ${data[4]}
fi