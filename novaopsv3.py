#!/usr/bin/env python
from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client
from novaclient import client
from collections import defaultdict
import os
import logging
import pprint
import json
#dont forget to source admin-openrch.sh file into env variables

USERNAME= os.environ['OS_USERNAME']
PASSWORD = os.environ['OS_PASSWORD']
AUTH_URL = os.environ['OS_AUTH_URL']
PROJECT_NAME = os.environ['OS_PROJECT_NAME']
VERSION = '2.30' #nova api version
logging.basicConfig(filename='novaops.log', level=logging.DEBUG, format='%(asctime)s %(message)s')



auth = v3.Password(auth_url=AUTH_URL,username=USERNAME,password=PASSWORD,project_name='demo',user_domain_id='default',project_domain_id='default')

sess = session.Session(auth=auth)
nova = client.Client(VERSION, session=sess)



def GetServers():		
	unsorted_hostlist = []
	hostlist = defaultdict(list)		
	for server in nova.servers.list(search_opts={'all_tenants': 1}):
		unsorted_hostlist.append([getattr(server, 'OS-EXT-SRV-ATTR:host', ''),server.id])	
	
	for host, serverid in unsorted_hostlist:
			hostlist[host].append(serverid)	
		
	return hostlist
		
		
		
		
def GetHosts():
	hostsonlylist = []
	#print ("Hosts to be touched:")
	for host in nova.hosts.list():
		if host.service == "compute":
			hostsonlylist.append(str(host)[7:-1])#cut string so only the actual hostname is saved to list, not <host: nodenamexy>,
		#print(host)	
	print("GetHosts Done")
	return hostsonlylist
	
def GetHostsJSON():
	hostsonlylistJSON = []
	#print ("Hosts to be touched:")
	for host in nova.hosts.list():
		if host.service == "compute":
			hostsonlylistJSON.append(str(host))
		#print(host)	
	print("GetHostsJSON Done")
	return hostsonlylistJSON

	
	
	
	
	
def preflight():
	writehosts()
	#loglist = open("hostlist.json","w")
	print('\n'+"Hosts and Servers to be touched:"+'\n')
	pprint.pprint(GetServers())	
	#pprint.pprint(GetHosts(), loglist) # write to file	
	#with open ("hostlist.json","w") as outfile:
		#json.dumps(GetHostsJSON(), outfile).encode('utf8')
	#loglist.close()
			
def hostparser():
	with open("hostlist.txt","r") as f:
		ops_hostlist = f.readlines()
		ops_hostlist = [x.strip() for x in ops_hostlist]	
	#print(*ops_hostlist, sep='\n')
	return ops_hostlist
	
	
def writehosts():
	loglist = open("hostlist.txt","w")
	for host in GetHosts():		
		loglist.write(host+'\n')
	loglist.close()
	
	
def migrateVM(vmid):
	nova.servers.live_migrate( block_migration='auto', server=vmid, host=None)
	
def enablehostmaintenance(host):
	return
	#enable maintenance mode for host here
	
def disablehostmaintenance(host):
	return
	#disable maintenance mode for host here
	
	
	
def ops():
	for host, servers in GetServers().items():
		if host in hostparser():
			nova.services.disable(host=host, binary="nova-compute")
			for s in servers:
				migrateVM(s)
			nova.services.enable(host=host, binary="nova-compute")
		#do migration
			return
	
	

	


#preflight()
#hostparser()
ops()




		
		
		
		
	