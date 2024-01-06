#!/bin/bash

# This script is used to monitor disk I/O performance

pid=$1

result_dir="../results/logs"

# get the newest directory in ../results/logs/
result_dir=$(ls -td ${result_dir}/*/ | head -1)
# get the newest file in result_dir, and remove the suffix
filename=$(ls -t ${result_dir} | head -1 | cut -d'.' -f1)

echo "Disk I/O Performance Test" $pid > ${result_dir}/${filename}.iostat

# Get the disk I/O performance for pid
iostat -d -x -m 1  >> ${result_dir}/${filename}.iostat &