"""
Boot up all hyperdex nodes including coordinator and servers
"""

import socket
import subprocess
import threading
import argparse
import time
import sys


RECONFIGURATOR = 'RC0'

ACTIVE_REPLICA = 'AR'

PORT = 6000

MAX = 200

# user name of cloudlab
USERNAME = 'oversky'

THREADS = 4


class CommandThread (threading.Thread):
    def __init__(self, host, cmd, username):
        threading.Thread.__init__(self)
        self.host = host
        self.cmd = cmd
        self.username = username

    def run(self):
        cmd = 'ssh '+ self.username+'@'+self.host+" "+self.cmd
        if cmd[-1] == '&':
            cmd += ''
        else:
            cmd += '&'
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print p.communicate(),self.host,'\n'


def get_actives(num):
    arr = []
    cnt = 0
    offset = 0
    for i in range(num):
        arr.append((ACTIVE_REPLICA+str(i+1), 'node-'+str(cnt+1), offset))
        cnt += 1
        if cnt == MAX:
            cnt = 0
            offset += 1
    return arr


def main():
    parser = argparse.ArgumentParser(description='Execute command on a list of remote machines')

    # The command to execute
    parser.add_argument('-r', '--replica', default=1, type=int,
                        help='Number of replicas')
    parser.add_argument('-p', '--partition', default=1, type=int,
                        help='Number of partitions')

    args = parser.parse_args()
    print args

    replica = args.replica
    partition = args.partition
    num = replica*partition

    actives = get_actives(num)

    th_pool = []

    # coordinator thread: coordinator is still undecided, let the first one to be the coordinator
    hostname = 'node-0'
    cmd = 'java -ea -cp CNS.jar -Djava.util.logging.config.file=conf/logging.properties'\
          + ' -Dlog4j.configuration=conf/log4j.properties'\
          + ' -DgigapaxosConfig=mongo-'+str(num)+'.properties edu.umass.cs.reconfiguration.ReconfigurableNode '+RECONFIGURATOR
    print cmd
    th = CommandThread(hostname, cmd, USERNAME)
    th.start()

    for tp in actives:
        ar, hostname, offset = tp
        # java -ea -cp CNS.jar -Djava.util.logging.config.file=conf/logging.properties
        # -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig=conf/examples/mongo.properties
        """
        cmd = 'taskset -c '+str(offset*THREADS)+'-' + str(offset*THREADS + THREADS-1) \
              + ' java -ea -Xms4G -Xmx4G -cp CNS.jar' \
              + ' -DTABLE='+str(ar) \
              + ' -Djava.util.logging.config.file=conf/logging.properties'\
              + ' -Dlog4j.configuration=conf/log4j.properties'\
              + ' -DgigapaxosConfig=mongo-'+str(num)+'.properties edu.umass.cs.reconfiguration.ReconfigurableNode '+str(ar)
        print cmd
        """

        cmd = 'java -ea -Xms8G -cp CNS.jar' \
              + ' -DTABLE='+str(ar) \
              + ' -Djava.util.logging.config.file=conf/logging.properties'\
              + ' -Dlog4j.configuration=conf/log4j.properties'\
              + ' -DgigapaxosConfig=mongo-'+str(num)+'.properties edu.umass.cs.reconfiguration.ReconfigurableNode '+str(ar)

        th = CommandThread(hostname, cmd, USERNAME)
        th_pool.append(th)
        th.start()

    time.sleep(10)
    print 'Experiment is booted up'


    sys.exit(0)


if __name__ == '__main__':
    main()
