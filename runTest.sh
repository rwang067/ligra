#!/bin/bash

# cd apps && make TestAll
BFS_RT=26737282

TEST=/home/cxy/ligra-mmap/inputs/rMatGraph_J_5_100
FS=/data1/zorax/datasets/Friendster/row_0_col_0/out.friendster

for data in $FS
do
    echo "Data: $data"
    # echo "-------original version---------"
    # ./apps/TestAll -r $BFS_RT $data
    # wait
    echo "---------mmap version-----------"
    ./apps/TestAll -map -r $BFS_RT $data
    wait
done