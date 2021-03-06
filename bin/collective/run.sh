#!/usr/bin/env bash

jar=../../target/mpi-stats-0.1-jar-with-dependencies.jar

col=$1
size=$2
itr=$3
p=$4

opts="-XX:+UseG1GC -Xms1G -Xmx1G"
btl="--mca btl tcp,sm,self --mca btl_tcp_if_include eth1"
$BUILD/bin/mpirun --report-bindings $btl -np $p --hostfile nodes.txt java $opts -cp ../target/$jar edu.iu.dsc.collectives.Program -collective $col -size $size -itr $itr  2>&1 | tee $summary
