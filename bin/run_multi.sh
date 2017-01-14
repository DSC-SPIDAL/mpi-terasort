#!/usr/bin/env bash

jar=../target/mpi-stats-0.1-jar-with-dependencies.jar
input=/scratch/skamburu/terasort/input_32
output=/scratch/skamburu/terasort/output/
partitionSampleNodes=10
partitionSamplesPerNode=100000
filePrefix=part
summary=summary.txt
sendBufferSize=500000
filesPerProcess=2
p=320
opts="-XX:+UseG1GC -Xms12G -Xmx12G"
#$BUILD/bin/mpirun --report-bindings --mca btl tcp,sm,self --mca btl_tcp_if_include eth1 --mca coll_tuned_use_dynamic_rules 1  --mca coll_tuned_gather_algorithm 1  -np $p --hostfile nodes.txt java $opts -cp ../target/$jar edu.iu.dsc.terasort.Program -input $input -output $output -partitionSampleNodes $partitionSampleNodes -partitionSamplesPerNode $partitionSamplesPerNode -filePrefix $filePrefix -sendBufferSize $sendBufferSize 2>&1 | tee $summary
time $BUILD/bin/mpirun -np $p --mca btl openib,sm,self --hostfile nodes.txt java $opts -cp $jar edu.iu.dsc.terasort.Program6 -input $input -output $output -partitionSampleNodes $partitionSampleNodes -partitionSamplesPerNode $partitionSamplesPerNode -filePrefix $filePrefix -sendBufferSize $sendBufferSize -filesPerProcess $filesPerProcess 2>&1 | tee $summary
