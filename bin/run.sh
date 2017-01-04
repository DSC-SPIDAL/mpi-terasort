#!/usr/bin/env bash

jar=../target/mpi-stats-0.1-jar-with-dependencies.jar
input=/home/supun/dev/projects/dsspidal/teragen/64/input/
output=/home/supun/dev/projects/dsspidal/teragen/64/output/
partitionSampleNodes=4
partitionSamplesPerNode=10000
filePrefix=part
summary=summary.txt
sendBufferSize=200000
p=4
opts="-XX:+UseG1GC -Xms3G -Xmx4G"
$BUILD/bin/mpirun --report-bindings -np $p --hostfile nodes.txt java $opts -cp ../target/$jar edu.iu.dsc.terasort.Program2 -input $input -output $output -partitionSampleNodes $partitionSampleNodes -partitionSamplesPerNode $partitionSamplesPerNode -filePrefix $filePrefix -sendBufferSize $sendBufferSize 2>&1 | tee $summary
