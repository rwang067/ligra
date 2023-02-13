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

# Chunk_data[0]=/mnt/nvme2/wr/friendster_chunks/out.friendster
# Chunk_data[6]=/mnt/nvme2/wr/yahooweb/out.yahooweb
Chunk_data[0]=/mnt/nvme2/zorax/Friendster/out.friendster
Chunk_data[1]=/mnt/nvme2/zorax/Twitter/xxx
Chunk_data[2]=/mnt/nvme2/zorax/Ukdomain/out.uk
Chunk_data[3]=/mnt/nvme2/zorax/Kron28/out
Chunk_data[4]=/mnt/nvme2/zorax/Kron29/out
Chunk_data[5]=/mnt/nvme2/zorax/Kron30/out
Chunk_data[6]=/mnt/nvme2/zorax/Yahoo/out
Chunk_data[7]=/mnt/nvme2/zorax/Kron31/xxx
Chunk_data[8]=/mnt/nvme2/zorax/Crawl/xxx

# echo $$ | sudo tee /sys/fs/cgroup/cxy-test/cgroup.procs

make clean; make cleansrc;
make testNebrs BFS PageRank Components KCore

# for idx in 7
for idx in {2,3,4,5,6}
do
    echo "---------Chunk version-----------"
    echo -n "Data: "
    echo ${Chunk_data[$idx]}
    echo -n "Root: "
    echo ${rts[$idx]}

    # sysctl -w vm.drop_caches=3
    echo "=======testNebrs======="
    ./testNebrs -b -chunk -debug -rounds 1 ${Chunk_data[$idx]}
    wait

    # sysctl -w vm.drop_caches=3
    echo "=======BFS======="
    ./BFS -b -r ${rts[$idx]} -chunk -rounds 1 ${Chunk_data[$idx]}
    wait

    # sysctl -w vm.drop_caches=3
    echo "=======Components======="
    ./Components -b -chunk -rounds 1 ${Chunk_data[$idx]}
    wait

    # sysctl -w vm.drop_caches=3
    echo "=======KCore======="
    ./KCore -b -chunk -rounds 1 ${Chunk_data[$idx]}
    wait

    # sysctl -w vm.drop_caches=3
    echo "=======PageRank======="
    ./PageRank -maxiters 10 -b -chunk -rounds 1 ${Chunk_data[$idx]}
    wait

    echo ""
done

# ## For debug ##
# make BFS
# ./BFS -b -r 35005211 -m -chunk -rounds 1 /mnt/nvme2/zorax/Friendster/out.friendster
# gdb ./BFS
# set args -b -r 35005211 -m -chunk -rounds 1 /mnt/nvme2/zorax/Friendster/out.friendster

# ./BFS -b -r 35005211 -m -chunk -rounds 1 /mnt/nvme2/wr/friendster_chunks/out.friendster
# ./PageRank -maxiters 10 -b -chunk -rounds 1 /mnt/nvme2/zorax/Friendster/out.friendster