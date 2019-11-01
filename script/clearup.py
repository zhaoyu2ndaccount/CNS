"""
Boot up all hyperdex nodes including coordinator and servers
"""

import socket
import subprocess
import threading
import argparse
import time
import sys


# user name of cloudlab
USERNAME = 'oversky'


class CommandThread (threading.Thread):
    def __init__(self, host, cmd, username):
        threading.Thread.__init__(self)
        self.host = host
        self.cmd = cmd
        self.username = username

    def run(self):
        cmd = 'ssh ' + self.username+'@'+self.host+' '+self.cmd
        if cmd[-1] == '&':
            cmd += ''
        else:
            cmd += '&'
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print p.communicate(),self.host,'\n'


def main():
    parser = argparse.ArgumentParser(description='Execute command on a list of remote machines')
    # The list of remote servers
    # parser.add_argument('file',
    #                     help='The file to read in of the server list')
    # The command to execute
    parser.add_argument('-n', '--num', default=1, type=int,
                        help='Number of hyperdex servers')
    args = parser.parse_args()
    print args

    num = args.num

    th_pool = []
    """
    fin = open('cl_ssh', 'r')
    for l in fin.readlines():
        hostname = l[:-1]
        cmd = 'pkill -f hyperdex'
        th = CommandThread(hostname, cmd, USERNAME)
        th_pool.append(th)
        if len(th_pool) > num:
            break
    fin.close()
    """

    for i in range(num+1):
        hostname = "node-"+str(i)
        cmd = 'pkill -f java'
        th = CommandThread(hostname, cmd, USERNAME)
        th_pool.append(th)

    for th in th_pool:
        th.start()

    for th in th_pool:
        th.join()

    time.sleep(1)

    th_pool = []
    for i in range(num+1):
        hostname = "node-"+str(i)
        cmd = 'rm -rf derby.log paxos_logs* reconfiguration_DB'
        th = CommandThread(hostname, cmd, USERNAME)
        th_pool.append(th)

    '''
    fin = open('cl_ssh', 'r')
    for l in fin.readlines():
        hostname = l[:-1]
        cmd = 'rm -f * data/* test_records'
        th = CommandThread(hostname, cmd, USERNAME)
        th_pool.append(th)
        if len(th_pool) > num:
            break
    fin.close()
    '''

    for th in th_pool:
        th.start()

    for th in th_pool:
        th.join()

    print 'Experiment has been cleared up'

    sys.exit(0)


if __name__ == '__main__':
    main()