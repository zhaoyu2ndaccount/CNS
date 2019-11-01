#!/bin/bash
folder=$1
num_rounds=5

mkdir -p data

# clear up
rm -rf data/*

# for i in {1..9}
# for total in 1 4 16 36 49 64 81 100 121 144
for total in 100
do	
	for r in $(seq 1 $num_rounds)
	do
		cat $folder/result-$total-0.5/0/round-$r-client-* | grep Thruput | awk '{sum += $2} END {print sum}' >> data/$total
	done
done
