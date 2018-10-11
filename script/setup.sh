#!/bin/bash

while read p; do
  echo $p
  scp jdk-8u181-linux-x64.tar.gz oversky@$p:
  scp CNS/jar/CNS.jar oversky@$p:
  scp .profile oversky@$p:
  scp .bashrc oversky@$p:
  scp test.sh oversky@$p:
  ssh oversky@$p "tar zxf jdk-8u181-linux-x64.tar.gz" < /dev/null &
done <$1

while read p; do
  echo $p
  ssh oversky@$p "mkdir -p conf; touch conf/testing.properties" < /dev/null &
done <$1

while read p; do
  scp CNS/conf/log* oversky@$p:conf
done <$1

while read p; do
  echo $p
  # ssh oversky@$p rm -rf result-* < /dev/null &
  # ssh oversky@$p ls < /dev/null &
  scp /etc/mongod.conf oversky@$p:
  ssh oversky@$p sudo mv mongod.conf /etc < /dev/null &
  ssh oversky@$p sudo service mongod restart < /dev/null &
done <$1

