import socket
import subprocess
import threading
import argparse
import time
import sys


ACTIVE_PREFIX = 'active'

DB_PREFIX = 'AR'

ACTIVE_REPLICA = 'AR'

# user name of cloudlab
USERNAME = 'oversky'

# shared folder
SHARED_FOLDER = '/proj/anolis-PG0/groups/'

MAX = 200

class CommandThread (threading.Thread):
    def __init__(self, host, cmd, username):
        threading.Thread.__init__(self)
        self.host = host
        self.cmd = cmd
        self.username = username

    def run(self):
        cmd = 'ssh ' + self.username+'@'+self.host+" "+self.cmd
        if cmd[-1] == '&':
            cmd += ''
        else:
            cmd += '&'
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        p.communicate()
        # print p.communicate(),self.host,'\n'

def get_nodes(num):
    fname = 'mongo-'+str(num)+'.properties'
    nodes = {}
    fin = open(fname)
    for l in fin:
        l = l[:-1]
        if l.startswith(ACTIVE_PREFIX):
            l = l.replace(ACTIVE_PREFIX+'.', '')
            l = l.split('=')
            name = l[0]
            node = l[1].split(':')[0]
            nodes[name] = node
    fin.close()
    return nodes


def main():
    parser = argparse.ArgumentParser(description='Execute command on a list of remote machines')

    # The command to execute
    parser.add_argument('-r', '--replica', default=1, type=int,
                        help='Number of replicas')
    parser.add_argument('-p', '--partition', default=1, type=int,
                        help='Number of partitions')

    args = parser.parse_args()
    # print args

    replica = args.replica
    partition = args.partition
    total = replica*partition

    th_pool = []

    for i in range(1, partition+1):
        cmd = 'mongodump --collection '+DB_PREFIX+str(i)+' --db active --out '+SHARED_FOLDER+str(total)+'-partition'
        th = CommandThread('node-'+str(i), cmd, USERNAME)
        th.start()
        th_pool.append(th)

    for th in th_pool:
        th.join()

    print 'All done: dump DB!'

if __name__ == '__main__':
    main()
