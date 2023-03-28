#!/bin/bash

export OMP_PROC_BIND=true

function set_schedule {
	SCHEDULE=$1
	export OMP_SCHEDULE="${SCHEDULE}"
}

profile_run() {
    eval commandargs="$1"
    eval filename="$2"
    
    commandname=$(echo $commandargs | awk '{print $1}')

    cur_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $cur_time "profile run with command: " $commandargs > ../${filename}.txt

    nohup $commandargs >> ../${filename}.log &
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

# clear_hugepages() { echo 0 > /proc/sys/vm/nr_hugepages; }
# open_hugepages() { echo 8192 > /proc/sys/vm/nr_hugepages; }

clear_hugepages() { echo "password" | sudo -S sysctl -w vm.nr_hugepages=0; }
open_hugepages() { 
    eval page_num="$1"
    echo "password" | sudo -S sysctl -w vm.nr_hugepages=$page_num;
}
# open_hugepages() { echo "password" | sudo -S sysctl -w vm.nr_hugepages=16384; }

# mkdir -p results_small

set_schedule "dynamic,64"

# roots:        TT       FS       UK      K28       K29       K30       YW       K31       CW
declare -a rts=(18225348 26737282 5699262 254655025 310059974 233665123 35005211 691502068 739935047)
declare -a d4kb=(8 18 16 16 30 32 33)
declare -a d2mb=(4 2 10 18 36 40 15)
declare -a c2mb=(1909 802 4325 8407 17583 34699 7495)

TEST_4KB=false
TEST_2MB=false
DEBUG=true

cd apps && make testNebrs BFS PageRank Components KCore
# make clean; make cleansrc;
# make testNebrs BFS PageRank Components KCore
# echo $$ | sudo tee /sys/fs/cgroup/cxy-test/cgroup.procs


# Chunk_data[0]=/mnt/nvme2/wr/friendster_chunks/out.friendster
# Chunk_data[6]=/mnt/nvme2/wr/yahooweb/out.yahooweb
# Chunk_data[0]=/mnt/nvme2/zorax/Friendster/out.friendster
# Chunk_data[1]=/mnt/nvme2/zorax/Twitter/xxx
# Chunk_data[2]=/mnt/nvme2/zorax/Ukdomain/out.uk
# Chunk_data[3]=/mnt/nvme2/zorax/Kron28/out
# Chunk_data[4]=/mnt/nvme2/zorax/Kron29/out
# Chunk_data[5]=/mnt/nvme2/zorax/Kron30/out
# Chunk_data[6]=/mnt/nvme2/zorax/Yahoo/out
# Chunk_data[7]=/mnt/nvme2/zorax/Kron31/xxx
# Chunk_data[8]=/mnt/nvme2/zorax/Crawl/xxx

cur_time=$(date "+%Y-%m-%d %H:%M:%S")
echo $cur_time 

if $TEST_4KB; then
    Chunk_data[0]=/mnt/nvme2/zorax/case4kb/Twitter/twitter
    Chunk_data[1]=/mnt/nvme2/zorax/case4kb/Friendster/friendster
    Chunk_data[2]=/mnt/nvme2/zorax/case4kb/Ukdomain/ukdomain
    Chunk_data[3]=/mnt/nvme2/zorax/case4kb/Kron28/kron28
    Chunk_data[4]=/mnt/nvme2/zorax/case4kb/Kron29/kron29
    Chunk_data[5]=/mnt/nvme2/zorax/case4kb/Kron30/kron30
    Chunk_data[6]=/mnt/nvme2/zorax/case4kb/Yahoo/yahoo

    clear_hugepages
    # open_hugepages
    
    max_times=1
    for ((times = 0; times < $max_times; times += 1 ))
    do
        # for idx in {0,1,2,3,4,5,6}
        for idx in {0,1}
        do
            echo "---------Chunk version-----------"
            echo -n "Data: "
            echo ${Chunk_data[$idx]}
            echo -n "Root: "
            echo ${rts[$idx]}
            pwd

            # # sysctl -w vm.drop_caches=3
            # echo "=======testNebrs======="
            # echo ./testNebrs -b -chunk -debug -rounds 1 ${Chunk_data[$idx]}
            # wait
    
            # sysctl -w vm.drop_caches=3
            echo "=======BFS======="
            echo ./BFS -b -r ${rts[$idx]} -chunk -rounds 1 ${Chunk_data[$idx]}
            wait

            # # # sysctl -w vm.drop_caches=3
            # echo "=======Components======="
            # ./Components -b -chunk -rounds 1 ${Chunk_data[$idx]}
            # wait

            # # # sysctl -w vm.drop_caches=3
            # echo "=======KCore======="
            # ./KCore -b -chunk -rounds 1 ${Chunk_data[$idx]}
            # wait

            # # # sysctl -w vm.drop_caches=3
            # echo "=======PageRank======="
            # ./PageRank -maxiters 10 -b -chunk -rounds 1 ${Chunk_data[$idx]}
            # wait
            # echo ""
        done
    done
    clear_hugepages
fi


if $TEST_2MB; then
    Chunk_data[0]=/mnt/nvme2/zorax/case2mb/Twitter/twitter
    Chunk_data[1]=/mnt/nvme2/zorax/case2mb/Friendster/friendster
    Chunk_data[2]=/mnt/nvme2/zorax/case2mb/Ukdomain/ukdomain
    Chunk_data[3]=/mnt/nvme2/zorax/case2mb/Kron28/kron28
    Chunk_data[4]=/mnt/nvme2/zorax/case2mb/Kron29/kron29
    Chunk_data[5]=/mnt/nvme2/zorax/case2mb/Kron30/kron30
    Chunk_data[6]=/mnt/nvme2/zorax/case2mb/Yahoo/yahoo

    cd apps && make clean && make testNebrs BFS PageRank Components KCore

    clear_hugepages

    max_times=3
    for ((times = 0; times < $max_times; times += 1 ))
    do
        for idx in {0,1,2,3,4,5,6}
        # for idx in {4,5,6}
        do
            # open_hugepages "\${c2mb[\$idx]}"
            echo "---------Chunk version-----------"
            echo -n "Data: "
            echo ${Chunk_data[$idx]}
            echo -n "Root: "
            echo ${rts[$idx]}

            # # sysctl -w vm.drop_caches=3
            # echo "=======testNebrs======="
            # ./testNebrs -b -chunk -debug -rounds 1 ${Chunk_data[$idx]}
            # wait

            # sysctl -w vm.drop_caches=3
            echo "=======BFS======="
            ./BFS -b -r ${rts[$idx]} -chunk -rounds 1 -d4kb ${d4kb[$idx]} -d2mb ${d2mb[$idx]} ${Chunk_data[$idx]}  >> res_2mb_mmap_bfs.txt
            wait

            # # sysctl -w vm.drop_caches=3
            echo "=======Components======="
            ./Components -b -chunk -rounds 1 -d4kb ${d4kb[$idx]} -d2mb ${d2mb[$idx]} ${Chunk_data[$idx]} >> res_2mb_mmap_cc.txt
            wait

            # # sysctl -w vm.drop_caches=3
            echo "=======KCore======="
            ./KCore -b -chunk -rounds 1 -d4kb ${d4kb[$idx]} -d2mb ${d2mb[$idx]} ${Chunk_data[$idx]} >> res_2mb_mmap_kc.txt
            wait

            # # sysctl -w vm.drop_caches=3
            echo "=======PageRank======="
            ./PageRank -maxiters 10 -b -chunk -rounds 1 -d4kb ${d4kb[$idx]} -d2mb ${d2mb[$idx]} ${Chunk_data[$idx]} >> res_2mb_mmap_pr.txt
            wait
            # clear_hugepages
            echo ""
        done
    done
fi

if $DEBUG; then
    # ## For debug ##
    
    # perf stat -e task-clock,cycles,instructions,cache-references,cache-misses,page-faults
    # perf record -e task-clock,cycles,instructions,cache-references,cache-misses,page-faults

    # ./BFS -b -r 26737282 -chunk -rounds 1 /mnt/nvme2/zorax/case4kb/Friendster/friendster 
    # ./PageRank -b -maxiters 10 -chunk -rounds 1 /mnt/nvme2/zorax/case4kb/Friendster/friendster
    # ./KCore -b -chunk -rounds 1 /mnt/nvme2/zorax/case4kb/Friendster/friendster
    # ./Components -b -chunk -rounds 1 /mnt/nvme2/zorax/case4kb/Friendster/friendster
    # ./BFS -b -r 233665123 -chunk -rounds 1 /mnt/nvme2/zorax/case4kb/Kron30/kron30
    # ./BFS -b -r 310059974 -m -chunk -rounds 1 /mnt/nvme2/zorax/case2mb/Kron29/kron29

    # ./testNebrs -b -m -chunk -rounds 1 /mnt/nvme2/wr/case2mb/Friendster/friendster
    # ./BFS -b -r 26737282 -m -chunk -rounds 1 /mnt/nvme2/wr/case4kb/Friendster/friendster
    # ./BFS -b -r 26737282 -m -chunk -rounds 1 /mnt/nvme2/wr/case2mb/Friendster/friendster

    # ./testNebrs -b -m -chunk -rounds 1 /mnt/nvme2/wr/case2mb/Kron29/kron29
    # ./BFS -b -r 310059974 -m -chunk -rounds 1 /mnt/nvme2/wr/case4kb/Kron29/kron29
    ./BFS -b -r 310059974 -m -chunk -rounds 1 /mnt/nvme2/wr/case2mb/Kron29/kron29

    # commandargs="./BFS -b -r 26737282 -chunk -rounds 1 /mnt/nvme2/zorax/case4kb/Friendster/friendster"
    # filename="friendster_chunk4kb"

    # profile_run "\${commandargs}" "\${filename}"
fi