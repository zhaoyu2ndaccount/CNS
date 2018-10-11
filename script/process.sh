#!/bin/bash
folder=$1
num=1
num_rounds=1

mkdir -p result

# clear up
rm result/*

for i in {1..9}
do	
	total=$(($num*$num))	
	for r in $(seq 1 $num_rounds)
	do
		cat $folder/result-$total/0.1/*/round-$r-client-* | grep Thruput | awk '{sum += $2} END {print sum}' >> result/$total
	done
	num=$(($num+1))
done
