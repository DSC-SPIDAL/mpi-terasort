#!/usr/bin/env bash

parts=3
outdir=/home/supun/dev/projects/dsspidal/teragen/64/output_multi
cat_files=""
space=" "
out=out

for p in $(seq 0 $parts);
    do
    echo valsort -o out${p}_0.sum $outdir/${p}_0
    valsort -o out${p}_0.sum $outdir/${p}_0
    cat_files=${cat_files}${out}${p}_0.sum${space}

    valsort -o out${p}_1.sum $outdir/${p}_1
    cat_files=${cat_files}${out}${p}_1.sum${space}
    valsort -o out${p}_2.sum $outdir/${p}_2
    cat_files=${cat_files}${out}${p}_2.sum${space}
done
echo $cat_files

cat $cat_files > all.sum
echo "validating all files"
valsort -s all.sum
