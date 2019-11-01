#!/bin/bash

folder=result
# num of attributes
attributes=10
num=1
coordinator=10.10.1.1
num_rounds=1
num_records=10000000


for num in 1 2 4 6 8 10 12
do	
	echo number of replica: 1
	echo number of partition: $num
	
	# generate config file
	echo 1. Generate config file
	python generate_config_file.py -r 1 -p $num
	
	# upload config file to all nodes
	echo 2. Upload config file
	total=$num
    cnt=0
	while read p; do
          echo $p
          scp mongo-$total.properties oversky@$p:
          if [ $cnt = $total ]
          then
            break
          fi
          cnt=$(($cnt+1))
    done < cl_ssh

	# boot up experiment
	echo 3. Boot up
	python bootup.py -r 1 -p $num &

	# wait for bootup successfully
	sleep 10

	# put records
	echo 4. Put 10M records
	java -ea -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=1 -Dnum_records=$num_records mongo.MongoAppSetupClient

: <<'END'
	echo 5. Run experiment!
	mkdir -p $folder/$frac
	for k in {1..1}
	do
		num_clients=10 #$(($n*20))
		echo round $k
		date
		for i in $(seq 1 $num_clients)
		do
			# python thru_client.py -a $coordinator -r 0 -f $frac &> $folder/$frac/round-$k-client-$i.log &
			java -ea -Xms2g -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=$num -Dratio=0.0 -Dfrac=$frac mongo.MongoAppSingleThreadThruputClient &> $folder/$frac/round-$k-client-$i.log &
			taskset -cp $(($i % 20)) $! > /dev/null
		done
		# while read p; do
			#echo $p
			# 0 means all search workload
			#ssh oversky@$p ./test.sh $k 0 < /dev/null &
		# done < client_ssh
		sleep 180
		echo round $k finished
		date
	done
	sleep 2
END

    mkdir -p /proj/anolis-PG0/groups/$total-partition

    sleep 2

	python dump.py -p $num -r 1

	sleep 2

	mv keys.txt /proj/anolis-PG0/groups/$total-partition

	./cleanup.sh $total

	sleep 2
done
