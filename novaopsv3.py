#!/usr/bin/env python
from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client
from novaclient import client
from collections import defaultdict
import os
import logging
import pprint
#dont forget to source admin-openrch.sh file into env variables

USERNAME= os.environ['OS_USERNAME']
PASSWORD = os.environ['OS_PASSWORD']
AUTH_URL = os.environ['OS_AUTH_URL']
PROJECT_NAME = os.environ['OS_PROJECT_NAME']
VERSION = '2' #nova api version
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
		hostsonlylist.append(host)
		#print(host)		
	return hostsonlylist
	print("GetHosts Done")
	
	
	
	
def ops():
	loglist = open("hostlist.txt","w")
	print('\n'+"Hosts and Servers to be touched:"+'\n')
	pprint.pprint(GetServers())
	pprint.pprint(GetHosts(), loglist) # write to file
	loglist.close()
		
		
GetServers()	
#GetHosts()
ops()



		
		
		
		
	