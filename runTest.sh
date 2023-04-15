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
    echo $cur_time "profile run with command: " $commandargs > ../result/${filename}.txt

    nohup $commandargs >> ../result/${filename}.txt &
    pid=$(ps -ef | grep $commandname | grep -v grep | awk '{print $2}')

    nohup /home/cxy/snoop-ligra/iosnoop.sh -p $pid > ../result/${filename}.snoop &
    perf record -e major-faults -p $pid -o ../result/${filename}.perf &
    wait $pid

    snoop_pid=$(ps -ef | grep trace_pipe | grep -v grep | awk '{print $2}')
    echo $snoop_pid
    kill -9 $snoop_pid
}

TEST=/home/cxy/ligra-mmap/inputs/rMatGraph_J_5_100
data[0]=/data1/zorax/datasets/Twitter/row_0_col_0/out.txt
data[1]=/data1/wr/datasets/Friendster/csr_bin/out.friendster
data[2]=/data1/wr/datasets/Ukdomain/csr_bin/out.uk
data[3]=/data1/wr/datasets/Kron28/csr_bin/out
data[4]=/data1/wr/datasets/Kron29/csr_bin/out
data[5]=/data1/wr/datasets/Kron30/csr_bin/out
data[6]=/data1/wr/datasets/Yahoo/csr_bin/out

name[0]=twitter
name[1]=friendster
name[2]=ukdomain
name[3]=kron28
name[4]=kron29
name[5]=kron30
name[6]=yahoo

# echo $$ | sudo tee /sys/fs/cgroup/cxy-test/cgroup.procs

cd apps && make BFS Components KCore PageRank BC MIS Radii
for idx in {0,1,2,3,4,5,6}
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
    # sysctl -w vm.drop_caches=3
    commandargs="./BC -b -r ${rts[$idx]} ${data[${idx}]}"
    filename="${name[${idx}]}_swap_bc"
    profile_run "\${commandargs}" "\${filename}"
    wait
    # sysctl -w vm.drop_caches=3
    commandargs="./MIS -b ${data[${idx}]}"
    filename="${name[${idx}]}_swap_mis"
    profile_run "\${commandargs}" "\${filename}"
    wait
    # sysctl -w vm.drop_caches=3
    commandargs="./Radii -b ${data[${idx}]}"
    filename="${name[${idx}]}_swap_rad"
    profile_run "\${commandargs}" "\${filename}"
    wait
done
