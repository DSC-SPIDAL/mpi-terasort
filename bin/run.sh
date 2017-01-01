#!/usr/bin/env bash

jar=../target/mpi-stats-0.1-jar-with-dependencies.jar
input=/scratch/skamburu/terasort/input/
output=/scratch/skamburu/terasort/output/
partitionSampleNodes=4
partitionSamplesPerNode=100000
filePrefix=part
summary=summary.txt
p=4

$BUILD/bin/mpirun --report-bindings --mca btl ^tcp -np $p --hostfile nodes.txt java $opts -cp ../target/$jar edu.iu.dsc.terasort.Program -input $input -output $output -partitionSampleNodes $partitionSampleNodes -partitionSamplesPerNode $partitionSamplesPerNode -filePrefix $filePrefix 2>&1 | tee $summary
