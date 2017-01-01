#!/usr/bin/env bash

outfolder=/scratch/skamburu/terasort/input

nodes=( j-038 j-039 j-040 j-041 j-042 j-043 j-044 j-045 j-046 j-047 j-048 j-049 j-050 j-051 j-052 j-053 )
procs=24
records_per_node=26041667

for i in "${nodes[@]}"
do
    start=0
    for p in $(seq 0 $procs);
        do
        echo $i $p
        echo ssh $i mkdir -p $outfolder
        ssh $i mkdir -p $outfolder
        echo ssh $i gensort -b$start $records_per_node $outfolder/part$p
        ssh $i gensort -b$start $records_per_node $outfolder/part$p
        start=$((start + records_per_node))
    done
done