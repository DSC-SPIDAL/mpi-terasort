#!/usr/bin/env bash

parts=3
outdir=/home/supun/dev/projects/dsspidal/teragen/64/output
cat_files=""
space=" "
out=out

for p in $(seq 0 $parts);
    do
    valsort -o out$p.sum $outdir/part$p
    cat_files=$cat_files$out$p.sum$space
done
echo $cat_files

cat $cat_files > all.sum
echo "validating all files"
valsort -s all.sum