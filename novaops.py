#!/usr/bin/env python
from __future__ import division
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


live_migration_completion_timeout = 800 #this needs to be the same value as live_migration_completion_timeout in your nova.conf on the compute nodes
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
    for server in nova.servers.list(search_opts={'all_tenants': 1, 'host': Host, 'status': 'PAUSED'}):
        ServerArray.append(server.id)
    return ServerArray
    
def GetInactiveServers(Host):
    ServerArray = []   
    for server in nova.servers.list(search_opts={'all_tenants': 1, 'host': Host, 'status': 'STOPPED'}):
        ServerArray.append(server.id)
    for server in nova.servers.list(search_opts={'all_tenants': 1, 'host': Host, 'status': 'SHUTOFF'}):
        ServerArray.append(server.id)
    for server in nova.servers.list(search_opts={'all_tenants': 1, 'host': Host, 'status': 'SUSPENDED'}):
        ServerArray.append(server.id)
    return ServerArray

def GetAllServers(Host):
    ServerArray = []   
    for server in nova.servers.list(search_opts={'all_tenants': 1, 'host': Host}):
        ServerArray.append(server.id)
    return ServerArray

def WriteHosts(aggregate):
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


def HostParser():
    with open("hostlist.txt", "r") as f:
        ops_hostlist = f.readlines()
        ops_hostlist = [x.strip() for x in ops_hostlist]
    return ops_hostlist




def RebootHost(host):
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


def LiveMigrateAction(vmid):
    for attempt in range(1):
        try:
            nova.servers.live_migrate(block_migration='auto', server=vmid, host=None)
        except:
            print('could not send live migration request to nova for server: ' +vmid + '  -> will try again in a moment.....' )
            time.sleep(10)
        else:
            break
    else:
        print('failed to send live migration request to nova for server: ' +vmid + '  -> will log in notmigrated.txt.....' )
        
def MigrateAction(vmid):
    for attempt in range(1):
        try:
            nova.servers.migrate(vmid)
        except:
            print('could not send migration request to nova for server: ' +vmid + '  -> will try again in a moment.....' )
            time.sleep(10)
        else:
            break
    else:
        print('failed to send migration request to nova for server: ' +vmid + '  -> will log in notmigrated.txt.....' )


def EnableHostMaintenance(host):
    nova.services.disable(host=host, binary="nova-compute")
    print('disabled nova-compute service on '+host)
    time.sleep(5)
    return


def DisableHostMaintenance(host):
    nova.services.enable(host=host, binary="nova-compute")
    print('enabled nova-compute service on '+host)
    time.sleep(5)
    return
    
def GetHostMemory(host):
    memory_mb = nova.hosts.get(host)[0].memory_mb    
    return memory_mb

def GetHostDisk(host):
    disk_gb = nova.hosts.get(host)[0].disk_gb
    return disk_gb
    
def GetHostMemoryUsed(host):
    memory_mb = nova.hosts.get(host)[1].memory_mb    
    return memory_mb
    
def GetHostDiskUsed(host):
    disk_gb = nova.hosts.get(host)[1].disk_gb
    return disk_gb

def GetEnvMemoryLoad():
    memory_mb = 0
    memory_mb_used = 0    
    for host in HostParser():
        memory_mb += GetHostMemory(host)
        memory_mb_used += GetHostMemoryUsed(host)        
    memory_gb = memory_mb/1000
    memory_gb_used = memory_mb_used/1000    
    memory_load = memory_gb_used/memory_gb
    return memory_load
   
def GetEnvDiskLoad():
    disk_gb = 0
    disk_gb_used = 0
    for host in HostParser():
        disk_gb += GetHostDisk(host)
        disk_gb_used += GetHostDiskUsed(host)
        disk_load = disk_gb_used/disk_gb
    return disk_load
    

def GetWorkerCount():
    if not argsdict['workers']:
        threadcount = math.ceil((len(HostParser()) * (1-GetEnvMemoryLoad())) / 2)
    else:
        threadcount = int(argsdict['workers'])
    return threadcount    
    

 
def CalcHostTimeout(host):
    memory_mb = GetHostMemoryUsed(host)
    disk_gb = GetHostDiskUsed(host)
    timeout = ((memory_mb*0.000931323) + (disk_gb*0.931323)) * live_migration_completion_timeout + 60 #0.000931323 is the factor from Megabyte to Gibibyte, The timeout is calculated based on the instance size, which is the instance's memory size in GiB. In the case of block migration, the size of ephemeral storage in GiB is added. The timeout in seconds is the instance size multiplied by the configurable parameter live_migration_completion_timeout, whose default is 800. For example, shared-storage live migration of an instance with 8GiB memory will time out after 6400 seconds. 
    return timeout
    
def CalcProcessTimeout():
    time = 0
    for host in HostParser():
        time += CalcHostTimeout(host)
    time = time/60/60/GetWorkerCount()
    return time

def CalcProcessTime():
    time = 0
    for host in HostParser():
        time += (GetHostDiskUsed(host) + (GetHostMemoryUsed(host)/1000))*10.1
    time = time/60/60/GetWorkerCount()
    return time
    
    
    
def CheckHostBusyMigrating(host):
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


def AppendNotMigratedServers(host):
    loglist = open("notmigrated.txt", "a")
    for server in remove_duplicates(GetAllServers(host)):
        loglist.write(host + ':\t' + server + '\t' + str(datetime.now()) + '\n')
        print(server+ ' might remain on '+host)
    loglist.close()

def LiveMigrateAllServersOnHost(host):
    for server in GetServers(host):
        LiveMigrateAction(server)
        print('Live Migrating Server '+server+' off '+host)
        time.sleep(1)
        

def MigrateAllServersOnHost(host):
    for server in GetInactiveServers(host):
        MigrateAction(server)
        print('Migrating Server '+server+' off '+host)
        time.sleep(1)


def CheckHostEmpty(host):
    if len(GetAllServers(host)) == 0:
        return 1
    else:
        return 0
 
def CheckHostUp(host):
    for service in nova.services.list(host=host):
        if service.binary == 'nova-compute':
            if service.state == 'up':
                return 1
            else:
                return 0


def LiveMigrationCleanup(host):
    for server in GetServers(host):
        for migration in nova.migrations.list(host=host,status='running',instance_uuid=server):
            if not server in live_migration_abort_submitted:
                live_migration_abort_submitted[server]=migration
                try:
                    print('Migration Timeout  -> try abort' + str(migration) +' for server: ' + server)
                    nova.server_migrations.live_migration_abort(server, migration)                    
                    print('migration abort submitted to nova for: '+server)             
                except:
                    print('Cannot abort live' + str(migration) +'for server: ' + server)
            else:
                print('migration abort already submitted to nova for: '+server +' waiting to abort...')


def CheckHostCompletion(host):
    if CheckHostEmpty(host):
        print(host + ' was evacuated successfully')
        return 1
    else:   
        host_timeout_counter = time.time()+CalcHostTimeout(host)
        time.sleep(15)
        
        while CheckHostBusyMigrating(host):
            print(host + ' not empty, waiting for migrations to complete...')
            time.sleep(30)            
            if time.time() > host_timeout_counter:
                LiveMigrationCleanup(host)
                break

                    
    if CheckHostEmpty(host):
        print(host + ' was evacuated successfully')
        # RebootHost(host)						#marked out for testing purposes#####################################
        while not CheckHostUp(host): #check if host has rebooted
            time.sleep(15)
        return 1
    else:
        AppendNotMigratedServers(host)
        print(host + ' was NOT evacuated successfully')
        return 0
        
        


def Worker(host):
    EnableHostMaintenance(host)
    LiveMigrateAllServersOnHost(host)
    MigrateAllServersOnHost(host)
    CheckHostCompletion(host)
    DisableHostMaintenance(host)
    return 1


def WorkerPool():
    pool_size = GetWorkerCount()
    pool = ThreadPool(pool_size) 
    print('Starting Worker Pool with '+ str(pool_size) +' Workers based on the fact that the Evironment has ' + str(GetEnvMemoryLoad()*100)+'% Memory Load')
    results = pool.map(Worker, HostParser())
    pool.close()
    pool.join()


if (args.list):
    WriteHosts(argsdict['aggregate'])
    if not argsdict['aggregate']:
        print("hostlist.txt was written without Aggregate Filter")
    else:
        print("hostlist.txt was written Aggregate ID Filter:" + argsdict['aggregate'])
else:
    print('This Process is expected to take a minimum of  '+str(CalcProcessTime()) + 'hours and will timeout after '+ str(CalcProcessTimeout())+' hours' ) 
    WorkerPool()
