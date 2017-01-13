#!/usr/bin/env bash

outfolder=/scratch/skamburu/terasort/input_32
resultsfolder=/scratch/skamburu/terasort/output_32

nodes="j-[035-065,077]"
procs=10
#records_per_node=17361112
records_per_node=31250000
#records_per_node=15625000

pdsh -w $nodes rm $outfolder/*
#pdsh -w $nodes rm $resultsfolder/*
start=0
for p in $(seq 0 $procs);
    do
    echo pdsh -w $nodes mkdir -p $outfolder
    pdsh -w $nodes mkdir -p $outfolder
    pdsh -w $nodes mkdir -p $resultsfolder
    echo pdsh -w $nodes gensort -b$start $records_per_node $outfolder/part$p
    pdsh -w $nodes gensort -b$start $records_per_node $outfolder/part$p
    start=$((start + records_per_node))
done
