#!/usr/local/bin/python

# This script is used to execute a command on a list of remote machines
# NOTE: whoever uses this script needs to make sure that you are able to access to the remote machine

import subprocess
import threading
import argparse
import time


class CommandThread (threading.Thread):
    def __init__(self, host, cmd, username):
        threading.Thread.__init__(self)
        self.host = host
        self.cmd = cmd
        self.username = username

    def run(self):
        cmd = 'ssh -o StrictHostKeyChecking=no '+ self.username+'@'+self.host+' '+self.cmd+''
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print p.communicate(),self.host,'\n'

def main():
    parser = argparse.ArgumentParser(description='Execute command on a list of remote machines')
    # The list of remote servers
    parser.add_argument('servers',
                        help='The list of remote servers to execute the command')
    # The user name to login
    parser.add_argument('-u', '--username', default='ubuntu',
                        help='The username to login into the remote servers')
    # The command to execute
    parser.add_argument('-c', '--command', default='ls',
                        help='The command to execute')
    args = parser.parse_args()
    print args

    # read in the list of hosts
    host_file = args.servers
    cmd = args.command
    username = args.username
    start = time.time()
    fin = open(host_file, 'r')
    th_pool = []
    for host in fin:
        host = host[:-1]
        if host[0] != '#':
            thr = CommandThread(host, cmd, username)
            th_pool.append(thr)
            thr.start()
    for th in  th_pool:
        th.join()

    fin.close()
    elapsed = time.time() - start
    print elapsed

if __name__ == '__main__':
    main()
