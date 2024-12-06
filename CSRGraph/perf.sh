#!bin/bash

# sudo perf record -F 99 -a -g -- sleep 10
sudo perf script > out.perf
~/ChunkExtra/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
~/ChunkExtra/FlameGraph/flamegraph.pl out.folded > perf.svg