#!/bin/bash
username=oversky

while read p; do
  echo $p
  scp jdk-8u181-linux-x64.tar.gz oversky@$p:
  scp CNS/jar/*.jar oversky@$p:
  scp .profile oversky@$p:
  scp .bashrc oversky@$p:
  # scp test.sh oversky@$p:
  ssh oversky@$p "tar zxf jdk-8u181-linux-x64.tar.gz" < /dev/null &
done <cl_ssh

while read p; do
  ssh $username@$p sudo mkdir /mydata < /dev/null
  ssh $username@$p sudo /usr/local/etc/emulab/mkextrafs.pl /mydata < /dev/null
done <cl_ssh

mongo_folder=/data/mongo

while read p; do
  echo $p
  ssh oversky@$p "mkdir -p conf; touch conf/testing.properties; sudo mkdir -p $mongo_folder" < /dev/null 
  ssh oversky@$p "sudo chmod 0777 $mongo_folder" < /dev/null
done <cl_ssh

while read p; do
  scp CNS/conf/*.jks CNS/conf/log* oversky@$p:conf
done <cl_ssh

while read p; do
  ssh oversky@$p sudo service mongod stop < /dev/null &
  ssh oversky@$p sudo rm -r /var/lib/mongodb/* < /dev/null &
done < cl_ssh

while read p; do
  echo $p
  # ssh oversky@$p rm -rf result-* < /dev/null &
  # ssh oversky@$p ls < /dev/null &
  scp mongod.conf oversky@$p:
  ssh oversky@$p sudo mv mongod.conf /etc < /dev/null &
  ssh oversky@$p sudo service mongod start < /dev/null &
done <cl_ssh

