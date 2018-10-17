#!/bin/bash

folder=result
# num of attributes
attributes=10
num=1
coordinator=node-0
num_rounds=3
num_records=10000000
# fraction
frac=`python -c "print '{0:.6f}'.format(50.0/$num_records)"`
# ./setup.sh cl_ssh

for n in {1..1}
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
	echo 4. Restore data from /proj/anolis-PG0/groups, get keys.txt file from /proj/anolis-PG0/groups, 
	java -ea -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=$num -Dnum_records mongo.MongoAppImportDBClient
	sleep 5
	# java -ea -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=$num -Dnum_records=$num_records mongo.MongoAppSetupClient
	echo Database has been restored!
	cp /proj/anolis-PG0/groups/$num-partition/keys.txt .
	

	echo 5. Run experiment!
	mkdir -p $folder/$frac

	for k in $(seq 1 $num_rounds)
	do
		num_clients=1 #$(($n*20))
		echo round $k
		date
		for i in $(seq 1 $num_clients)
		do
			# java -ea -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=$num -Dratio=0.0 -Dfrac=$frac mongo.MongoAppThruputClient &> $folder/$frac/round-$k-client-$i.log 
			java -ea -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=$num -Dratio=0.0 -Dfrac=$frac mongo.MongoAppCapacityProbingClient &> $folder/$frac/round-$k-client-$i.log
			# :taskset -cp $((($i-1)*4))-$((($i-1)*4+3)) $! > /dev/null
		done
		sleep 20
		echo round $k finished
		date
		pkill -f MongoAppThruputClient
	done
	sleep 2


: <<'END'
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
END
	
	#clear up
	echo 6. Clear up
	java -ea -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=$num mongo.MongoAppCleanupClient
	sleep 2
	
	python remote_block_execute.py -u oversky -c "mongo active --eval 'db.dropDatabase()' " cl_ssh 		
	sleep 2

	python clearup.py -n $total
	sleep 2 # $((10+$num*2))

	mv result result-$total	

	# update number of servers
	num=$(($num+1))
done
