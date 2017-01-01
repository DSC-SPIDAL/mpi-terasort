#!/usr/bin/env bash

outfolder=/scratch/skamburu/terasort/input

nodes="j-0[39-53]"
procs=24
records_per_node=26041667

start=0
for p in $(seq 0 $procs);
    do
    echo pdsh -w $nodes mkdir -p $outfolder
    pdsh -w $nodes mkdir -p $outfolder
    echo pdsh -w $nodes gensort -b$start $records_per_node $outfolder/part$p
    pdsh -w $nodes gensort -b$start $records_per_node $outfolder/part$p
    start=$((start + records_per_node))
done
