#!/bin/bash

folder=result
# num of attributes
attributes=10
num=1
coordinator=node-0
num_rounds=1
num_records=10000000
# fraction
frac=`python -c "print '{0:.6f}'.format(100.0/$num_records)"`
# ./setup.sh cl_ssh

for n in {1..2}
do	
	echo number of replica: $num
	echo number of partition: $num
	
	# generate config file
	echo 1. Generate config file
	python generate_config_file.py -r $num -p $num
	
	# upload config file to all nodes
	echo 2. Upload config file
	total=$(($num*$num))

	while read p; do
                echo $p
                scp mongo-$total.properties oversky@$p:
                #ssh oversky@$p mkdir -p cns/result/$frac < /dev/null
        done < cl_ssh

	# boot up experiment
	echo 3. Boot up
	python bootup.py -r $num -p $num &

	# wait for bootup successfully	
	sleep $(($total+5))
	
	# put records
	echo 4. Put 100k records
	java -ea -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=$num -Dnum_records=$num_records mongo.MongoAppSetupClient

	# upload keys to all clients	
	while read p; do
		echo $p
		scp keys.txt oversky@$p:
		#ssh oversky@$p mkdir -p hyperdex/result/$frac < /dev/null
	done < client_ssh

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
	echo 5. Run experiment on all clients!
	for k in $(seq 1 $num_rounds)
	do
		echo round $k
		while read p; do
			echo $p
                	ssh oversky@$p ./test.sh $num $k $frac < /dev/null &
		done < client_ssh
		sleep 180
	done
	sleep 2
	
	#clear up
	echo 6. Clear up
	java -ea -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=$num mongo.MongoAppCleanupClient
	pkill -f edu.umass.cs.reconfiguration.ReconfigurableNode
	pkill -f bootup.py
	# rm keys.txt
	# mv result result-$total
	
	while read p; do
		ssh oversky@$p mv result result-$total < /dev/null &
	done < client_ssh
	python clearup.py -n $total
	sleep 10 # $((10+$num*2))
	
	echo 7. Collect result
	while read p;do
		mkdir -p $folder/$frac/$p
		scp oversky@$p:result-$total/$frac/round-*-client-*.log $folder/$frac/$p/
	done < client_ssh

	mv result result-$total	

	while read p;do
                ssh oversky@$p rm -rf result-$total < /dev/null &
        done < client_ssh
	# number of servers
	num=$(($num+1))
done
