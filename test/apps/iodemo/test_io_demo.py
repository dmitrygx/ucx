#!/usr/bin/python
#
# Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
#
# See file LICENSE for terms.
#

import subprocess
import sys
import argparse
import os
import commands
import time
import socket
import selectors

def launch_process(hostname, cmd, out_file, addr) :
    commands.getstatusoutput("export -p > io_demo_env.sh")
    commands.getstatusoutput("chmod u+x io_demo_env.sh")
    io_demo_env_cmd = ". " + os.path.abspath(os.getcwd()) + "/io_demo_env.sh"
    f = open(out_file, "wb")
    proc_cmd = " \'" + io_demo_env_cmd + "; " + cmd + " -f " + addr + "\'"
    launch_cmd = "pdsh -w " + hostname + proc_cmd
    print("launching: " + launch_cmd)
    p = subprocess.Popen([launch_cmd],
                         stdout=f,
                         stderr=f,
                         env=os.environ,
                         shell=True)
    time.sleep(5)
    commands.getstatusoutput("rm -rf io_demo_env.sh")
    return p

def stop_process(proc) :
    proc.terminate()

parser = argparse.ArgumentParser(description='TODO: description')

parser.add_argument('--client_host',
                    help='TODO')
parser.add_argument('--server_host',
                    help='TODO')
parser.add_argument('--client_cmd',
                    help='TODO')
parser.add_argument('--server_cmd',
                    help='TODO')

args = parser.parse_args()

port = 9090
sock = socket.socket()

ip_addr = socket.gethostbyname(socket.gethostname())

sock.bind((ip_addr, port))
sock.listen(2)

addr = ip_addr + ":" + str(port)
print(addr)

client_proc = launch_process(args.client_host, args.client_cmd,
                             "io_demo_client.out", addr)
client_conn, client_addr = sock.accept()
print("accepted connection from client - " + str(client_addr))

client_conn.setblocking(False)
sel.register(client_conn, selectors.EVENT_READ, read)

server_proc = launch_process(args.server_host, args.server_cmd,
                             "io_demo_server.out", addr)
server_conn, server_addr = sock.accept()
print("accepted connection from server - " + str(server_addr))


stop_process(client_proc)
stop_process(server_proc)

