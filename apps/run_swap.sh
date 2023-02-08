#!/bin/bash

export OMP_PROC_BIND=true

function set_schedule {
	SCHEDULE=$1
	export OMP_SCHEDULE="${SCHEDULE}"
}

#mkdir -p results_small

set_schedule "dynamic,64"

# cd apps && make TestAll
declare -a rts=(26737282 18225348 5699262 254655025 310059974 233665123 35005211 691502068 739935047)

TEST=/home/cxy/ligra-mmap/inputs/rMatGraph_J_5_100
CSR_data[0]=/data1/wr/datasets/Friendster/csr_bin/out.friendster
CSR_data[1]=/data1/zorax/datasets/Twitter/row_0_col_0/out.txt
CSR_data[2]=/data1/wr/datasets/Ukdomain/csr_bin/out.uk
CSR_data[3]=/data1/wr/datasets/Kron28/csr_bin/out
CSR_data[4]=/data1/wr/datasets/Kron29/csr_bin/out
CSR_data[5]=/data1/wr/datasets/Kron30/csr_bin/out
CSR_data[6]=/data1/wr/datasets/Yahoo/csr_bin/out
CSR_data[7]=/data2/graph/Kron31/csr_bin/out
CSR_data[8]=/data1/wr/datasets/Crawl/csr_bin/crawl

# echo $$ | sudo tee /sys/fs/cgroup/cxy-test/cgroup.procs

make clean; make cleansrc;
make testNebrs BFS PageRank Components KCore

for idx in 0
do
    echo "---------Swap version-----------"
    echo -n "Data: "
    echo ${CSR_data[$idx]}
    echo -n "Root: "
    echo ${rts[$idx]}

    # sysctl -w vm.drop_caches=3
    echo "=======testNebrs======="
    ./testNebrs -b -rounds 1 ${CSR_data[$idx]}
    wait

    # sysctl -w vm.drop_caches=3
    echo "=======BFS======="
    ./BFS -b -r ${rts[$idx]} -rounds 1 ${CSR_data[$idx]}
    wait

    # sysctl -w vm.drop_caches=3
    echo "=======Components======="
    ./Components -b -rounds 1 ${CSR_data[$idx]}
    wait

    # sysctl -w vm.drop_caches=3
    echo "=======KCore======="
    ./KCore -b -rounds 1 ${CSR_data[$idx]}
    wait

    # sysctl -w vm.drop_caches=3
    echo "=======PageRank======="
    ./PageRank -b -rounds 1 ${CSR_data[$idx]}
    wait

    echo ""
done
