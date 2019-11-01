#!/bin/bash

folder=result
# num of attributes
attributes=10
coordinator=node-0
num_rounds=5
num_records=10000000
# fraction


# for num in 1 2 3 4 5 6 7 8 10 11 12
# for num in 6 7 8 9 11 12 1 2 4
for num in 10
do	
	echo number of replica: $num
	echo number of partition: $num
	
	# generate config file
	echo 1. Generate config file
	python generate_config_file.py -r $num -p $num
	
	# upload config file to all nodes
	echo 2. Upload config file
	total=$(($num*$num))

	cnt=0
	while read p; do
                echo $p
                scp mongo-$total.properties oversky@$p:
                if [ $cnt = $total ]
                then
                    break
                fi
                cnt=$(($cnt+1))
                #ssh oversky@$p mkdir -p cns/result/$frac < /dev/null
	 done < cl_ssh

    python load_data.py -p $num -r $num
    # cp /proj/anolis-PG0/groups/$total-partition/keys.txt .

    # boot up experiment
    echo 3. Boot up
    python bootup.py -r $num -p $num &

    # wait for bootup successfully
    sleep 60

    # put records
    echo 4. Create all service names and restore DBs # Restore data from /proj/anolis-PG0/groups, get keys.txt file from /proj/anolis-PG0/groups,
    # restore DBs
    if [ $num -eq 0 ]
    then
        python load_data.py -p $num -r $num
    fi
    # create service names
    java -ea -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=$num mongo.MongoAppImportDBClient
    sleep 10

    echo Database has been restored!
    cp /proj/anolis-PG0/groups/$num-partition/keys.txt .

    echo 5. Run experiment!

    frac=`python -c "print '{0:.6f}'.format(50.0/$num_records)"`
    ratio=0.5
    attr=0
    init_probing_load=$(($num*50/4))

    mkdir -p $folder/$attr

    for k in $(seq 1 $num_rounds)
    do
        # java -ea -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=$num mongo.MongoAppImportDBClient

        echo round $k
        date
        java -ea -cp ./CNS/jar/CNS.jar -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=mongo-$total.properties -DnumPartition=$num -DnumReplica=$num -Dratio=$ratio -Dfrac=$frac -Dselected=$attr mongo.MongoAppCapacityProbingClient $init_probing_load &> $folder/$attr/round-$k-client-1.log

        echo round $k finished
        date
        pkill -f MongoAppCapacityProbingClient

    done
    mv result result-$total-$ratio


    #clear up
    echo 6. Clear up
    ./cleanup.sh $total
    # python clearup.py -n $total
    sleep 30 # $((10+$num*2))

done
