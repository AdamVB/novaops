#!/usr/bin/env python
import argparse
import sys
import math
import threading
import time
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool
import functools
print = functools.partial(print, flush=True) #So Printoutput isn't buffered but flushed when invoked


from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client
from novaclient import client
from collections import defaultdict
import os
import logging
import pprint
import json
import paramiko

# dont forget to source admin-openrc.sh file into env variables


parser = argparse.ArgumentParser(
    description='This Script will restart every Compute Node/Host for patching and Updating, it will also Live Migrate the Servers off the Hosts before Restarting')
parser.add_argument('-l', '--list', action='store_true',
                    help='This Option will only write the List of Hosts and not do any Server Operations',
                    required=False)
# parser.add_argument('-az','--availabilityzone', help='Enter AZ Name here to filter Hosts by that particular AZ', required=False)
parser.add_argument('-ag', '--aggregate',
                    help='Enter Aggregate ID (Integer) here to filter Hosts by that particular Host Aggregate',
                    required=False, type=int)
parser.add_argument('-w', '--workers',
                    help='Enter Number of concurrent workers that the Program should use. The Number of workers is how many servers will be live evacuated at once. ',
                    required=False, type=int)

args = parser.parse_args()
argsdict = vars(args)


USERNAME = os.environ['OS_USERNAME']
PASSWORD = os.environ['OS_PASSWORD']
AUTH_URL = os.environ['OS_AUTH_URL']
PROJECT_NAME = os.environ['OS_PROJECT_NAME']
VERSION = '2.30'  # nova api version
logging.basicConfig(filename='novaops.log', level=logging.DEBUG, format='%(asctime)s %(message)s')

auth = v3.Password(auth_url=AUTH_URL, username=USERNAME, password=PASSWORD, project_name=PROJECT_NAME,
                   user_domain_id='default', project_domain_id='default')
sess = session.Session(auth=auth)
nova = client.Client(VERSION, session=sess)


LIVE_MIGRATION_TIMEOUT = 180
live_migration_abort_submitted = {}



def remove_duplicates(l):  # Necessary because Hosts can be part of multiple Host Aggregates
    return list(set(l))


def GetAggregates():
    AggregatesArray = []
    for aggregate in nova.aggregates.list():
        AggregatesArray.append(aggregate.id)
    return AggregatesArray


def GetHostsInAggregate(aggregate):
    HostsInAggregatesArray = []
    for index in getattr(nova.aggregates.get_details(aggregate), 'hosts'):
        HostsInAggregatesArray.append(index)
    return HostsInAggregatesArray


def GetServers(Host):
    ServerArray = []
    for server in nova.servers.list(search_opts={'all_tenants': 1, 'host': Host, 'status': 'ACTIVE'}):
        ServerArray.append(server.id)
    return ServerArray


def writehosts(aggregate):
    HostListToWrite = []
    if not aggregate:
        for a in GetAggregates():
            for h in GetHostsInAggregate(a):
                HostListToWrite.append(h)

    else:
        for h in GetHostsInAggregate(aggregate):
            HostListToWrite.append(h)

    loglist = open("hostlist.txt", "w")
    for host in remove_duplicates(HostListToWrite):
        loglist.write(host + '\n')
    loglist.close()


def hostparser():
    with open("hostlist.txt", "r") as f:
        ops_hostlist = f.readlines()
        ops_hostlist = [x.strip() for x in ops_hostlist]
    return ops_hostlist


def getworkercount():
    if not argsdict['workers']:
        threadcount = math.ceil((len(hostparser()) * 0.1) / 2)
    else:
        threadcount = int(argsdict['workers'])
    return threadcount


def rebootHost(host):
    # nova.hosts.host_action.reboot(host) not possible anymore because these low level actions have been removed from compute service, instead reboot via ssh
    #paramiko also supports SSH with Key Certificates. See paramiko documentation for this.
    print('Rebooting Host:'+host)
    sshusername = 'stack'
    sshpassword = 'devstack'
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, 22, username=sshusername, password=sshpassword, timeout=20)
    try:
        ssh.exec_command('sudo reboot')
    except AuthenticationException as e:
        e = str(e)
        print(e)
    finally:
        ssh.close()


def migrateAction(vmid):
    nova.servers.live_migrate(block_migration='auto', server=vmid, host=None)


def enablehostmaintenance(host):
    nova.services.disable(host=host, binary="nova-compute")
    print('disabled nova-compute service on '+host)
    time.sleep(15)
    return


def disablehostmaintenance(host):
    nova.services.enable(host=host, binary="nova-compute")
    print('enabled nova-compute service on '+host)
    time.sleep(15)
    return


def checkHostBusyMigrating(host):
    if len(nova.servers.list(search_opts={'all_tenants': 1, 'host': host, 'status': 'MIGRATING'})) > 0:
        return 1
    elif len(nova.migrations.list(host=host,status='queued')) > 0:
        return 1
    elif len(nova.migrations.list(host=host,status='preparing')) > 0:
        return 1
    elif len(nova.migrations.list(host=host,status='running')) > 0:
        return 1
    else:
        return 0


def appendNotMigratedServers(host):
    loglist = open("notmigrated.txt", "a")
    for server in remove_duplicates(GetServers(host)):
        loglist.write(host + ':\t' + server + '\t' + str(datetime.now()) + '\n')
        print(server+ ' might remain on '+host)
    loglist.close()


def livemigrateallserversonhost(host):
    for server in GetServers(host):
        migrateAction(server)
        print('Migrating Server '+server+' off '+host)
        time.sleep(5)


def checkHostEmpty(host):
    if len(GetServers(host)) == 0:
        return 1
    else:
        return 0


def livemigrationcleanup(host):
    for server in GetServers(host):
        for migration in nova.migrations.list(host=host,status='running',instance_uuid=server):
            if not server in live_migration_abort_submitted:
                live_migration_abort_submitted[server]=migration
                try:
                    print('Migration Timeout -> try abort' + str(migration) +' for server: ' + server)
                    nova.server_migrations.live_migration_abort(server, migration)                    
                    print('migration abort submitted to nova for: '+server)             
                except:
                    print('Cannot abort live' + str(migration) +'for server: ' + server)
            else:
                print('migration abort already submitted to nova for: '+server +' waiting to abort...')


def checkhostcompletion(host):
    if checkHostEmpty(host):
        print(host + ' was evacuated successfully')
        return 1
    else:   
        host_timeout_counter = time.time()+LIVE_MIGRATION_TIMEOUT * len(GetServers(host)) * 2
        timeout_counter = time.time()+LIVE_MIGRATION_TIMEOUT
        time.sleep(15)
        
        while checkHostBusyMigrating(host):
            print(host + ' not empty, waiting for migrations to complete...')
            time.sleep(30)            
            if time.time() > timeout_counter:
                livemigrationcleanup(host)                
                timeout_counter = time.time()+LIVE_MIGRATION_TIMEOUT
                if time.time() > host_timeout_counter:
                    break
                    
    if checkHostEmpty(host):
        print(host + ' was evacuated successfully')
        # rebootHost(host)						#marked out for testing purposes#####################################
        return 1
    else:
        appendNotMigratedServers(host)
        print(host + ' was NOT evacuated successfully')
        return 0


def worker(host):
    enablehostmaintenance(host)
    livemigrateallserversonhost(host)
    checkhostcompletion(host)
    disablehostmaintenance(host)
    return 1


def WorkerPool():
    pool_size = getworkercount()
    pool = ThreadPool(pool_size)
    print('Starting Worker Pool with '+ str(pool_size) +' workers')
    results = pool.map(worker, hostparser())
    pool.close()
    pool.join()


if (args.list):
    writehosts(argsdict['aggregate'])
    if not argsdict['aggregate']:
        print("hostlist.txt was written without Aggregate Filter")
    else:
        print("hostlist.txt was written Aggregate ID Filter:" + argsdict['aggregate'])
else:
    WorkerPool()

