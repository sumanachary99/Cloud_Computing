#!/usr/bin/env python
import pika
import sys
from flask import Flask, request, abort, render_template, jsonify, Response
import sqlite3
import requests
import re
import datetime
import json
import docker
import time
import uuid
import threading
import os
import subprocess
from kazoo.client import KazooClient
from kazoo.client import EventType
import logging
# Connect to zookeeper
logging.basicConfig()
zk = KazooClient(hosts='zoo:2181')
zk.start()

c=0
prevZnodeDataWatchCalledOn=""
prevEventType=""
app = Flask(__name__)
client = docker.from_env()
ischeckingReqCount = False
activeSlaves = 1
workerCount=1
crashSlaveApiCalled=False
count = 0
readReq = False  # true: writereq false : readReq
zk_path="/producer/"
zk.ensure_path(zk_path)
scalingDown=False
allZnodes={}
znodesCount=0
pidZnodeMapping={}
currentMasterZnodePath="/producer/Worker0"
currentMasterpid=0
slavesDeletedDueToScaleDown=0


#This function creates worker containers .
#  The argument specifies the number of container to be created.
def createSlaves(noOfSlavesShouldbeCreated):
    global c
    global pidZnodeMapping
    global activeSlaves
    global workerCount
    client = docker.from_env()
    print("\n\n")
    print(noOfSlavesShouldbeCreated, "noOfSlaves going to be created")
    #network = client.networks.create("cc_proj_local_network", driver="bridge")
    activeSlaves = activeSlaves+noOfSlavesShouldbeCreated

    # Loop until all containers are created.
    for i in range(0,noOfSlavesShouldbeCreated):
        workerCount+=1
        # using docker sdk spawn containers with the network attached to the default network(cc_proj_local_network)
        container = client.containers.run(
            "slave", detach=True, network="cc_proj_local_network",environment=["workerUniqueId="+str(workerCount)],stop_signal="SIGINT")
        # Using docker sdk low level api. Get the pid of the container spawned and map it 
        # -with the znode path of the new worker
        contId=container.id
        apiClient=docker.APIClient()
        data = apiClient.inspect_container(contId)
        contPid=data['State']['Pid']
        print(contPid,"PID of new Container spawned")
        pidZnodeMapping[contPid]="/producer/Worker"+str(workerCount)
        print(pidZnodeMapping)

        # {121:producer/worke0,12131:producer/worker2}




# This function deletes worker containers. Argument specifies the number of containers to be killed.
# This is used to maintain scalability.
def deleteSlave(noOfSlavesToBeDeleted):
   
    print('\nKilling '+str(noOfSlavesToBeDeleted)+' slaves\n')
    global activeSlaves
    global scalingDown
    global  slavesDeletedDueToScaleDown
    for i in range(noOfSlavesToBeDeleted):
        slavesDeletedDueToScaleDown+=1
        scalingDown=True
        activeSlaves -= 1
        # Call the crash slave api to crash the container
        requests.post(url='http://127.0.0.1/api/v1/crash/slave')
    scalingDown=False



# This function is used in maintaining the scalability.
# This function is called on the start up of this container.
# This checks the request count every 2 minutes and scales up or down accordingly.
def checkReqCount():
    global count
    sleepSec = 120
    time.sleep(sleepSec)

    # Create a infinite loop called every 2 minutes.
    while(1):
        requestMultiple = 20
        print('\n\ncalled after 2 min to reset count and count is :', count)
        # Get the number of working containers list and decide how many containers should be spawned or crashed.
        pids = requests.get(url='http://127.0.0.1/api/v1/worker/list')
        print(pids,"Pids")
        pidList = json.loads(pids.text)
        activeSlaves = len(pidList)-1
        print(activeSlaves, "ActiveSlaves\n")
        try:
            noOfSlavesShouldbeAlive = count//requestMultiple
            if(count//requestMultiple != count/float(requestMultiple)):
                noOfSlavesShouldbeAlive += 1
            if(count == 0):
                noOfSlavesShouldbeAlive = 1
            # Scale out ,create new containers
            if(noOfSlavesShouldbeAlive > activeSlaves):
                    createSlaves(noOfSlavesShouldbeAlive-activeSlaves)
            # Scale in , delete some containers
            elif noOfSlavesShouldbeAlive < activeSlaves:
                    print('')
                    deleteSlave(activeSlaves-noOfSlavesShouldbeAlive)
        except Exception as e:
            print('Exception in every 2 min scale in and out loop')
        count = 0
        time.sleep(sleepSec)

# This funcion is used to perform leader election
# And spawn a new container to componsate the crash of master.
def electLeader():
    global zk
    global currentMasterZnodePath
    global currentMasterpid
    pids = requests.get(url='http://127.0.0.1/api/v1/worker/list')
    print(pids,"Pids")
    pidList = json.loads(pids.text)
    
    # From the worker list get -
    # - the slave pid who is the oldest and make it master.
    newMasterPid=pidList[0]
    currentMasterZnodePath=pidZnodeMapping[newMasterPid]
    print("\nThis guy is new master",newMasterPid)
    print("This guy is new master",currentMasterZnodePath)
    currentMasterpid=newMasterPid
    
    # Set the data of the znode to trigger the watch inside the slave container.
    zk.set(currentMasterZnodePath,b"master")
    print("Creating slave after electing leader")
    createSlaves(1)

# Confirm if the master died or slave died and take decision accordingly.
def checkIfMasterDied(event):
    global currentMasterZnodePath
    global scalingDown
    global slavesDeletedDueToScaleDown
    print('\n\nchecking if master died . . .')
    print(event.path,"Event.path")
    # If the znode path of deleted event is same as the present master znode path.
    # Then confirm that master has died.
    if(event.path==currentMasterZnodePath):
        print("[x] Checking done.")
        print("Yes its true that master died :(")
        print("Lets elect our new king .")
        electLeader()
    else:
        print("Chill dude ! Master dint die :)")
        if slavesDeletedDueToScaleDown==0:
            print("creating slave due to fault tolerance")   
            createSlaves(1)
        if(slavesDeletedDueToScaleDown>0):
            slavesDeletedDueToScaleDown-=1
            # scalingDown=False



# Function to add DataWatch on each worker znode.
def foo(znode):
    @zk.DataWatch("/producer/"+znode)
    def watch_children_data(data, stat, event):
        print("\nDataWatch Called")
        print(data,"data\n")
        print(event,"event\n")
        global prevEventType
        global prevZnodeDataWatchCalledOn
        
        if(event!=None):
            # If znode is deleted and same watch is called again and again then return without any action
            if(prevEventType=="deleted" and prevZnodeDataWatchCalledOn==event.path):
                return 
            else:
                if(event.type==EventType.DELETED):
                    # If znode is deleted then find if master died
                    prevEventType="deleted"
                    prevZnodeDataWatchCalledOn=event.path
                    print("Some worker Crashed!")
                    checkIfMasterDied(event)


# Adding DataWatch on the root znode path
# It helps in adding dataWatch on newly created znodes 
@zk.ChildrenWatch("/producer/")
def watch_children(children):
    global crashSlaveApiCalled
    global allZnodes    
    global znodesCount
    global scalingDown
    global c
    
    print("\n\nIn orchestrator watch , Children are now: %s" % children)
    for znode in children:
        if(znode not in allZnodes):
            allZnodes[znode]=0
            foo(znode)
            znodesCount+=1
    
    print(allZnodes,"AllZnodes List")
    # if there is any reduction in number of znodes -
    # -then detect that some znode or container has crashed.
    if(znodesCount>len(children)):
        znodesDeleted=abs(len(children)-znodesCount)
        print(str(znodesDeleted)+" Znodes deleted:( \n")
        znodesCount=len(children)

# Class to help with message queues using RPC
class RpcClient(object):
    def __init__(self, readReq, responseQueueName, messageQueueName):
        self.readRequest = readReq
        self.messageQueue = messageQueueName
        self.responseQueueName = responseQueueName
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue="",exclusive=True,durable=True)
        # result = self.channel.queue_declare(queue=responseQueueName,durable=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,)

    def on_response(self, ch, method, props, body):       
        if self.corr_id == props.correlation_id:
            self.response = json.loads(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def call(self, data):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=self.messageQueue,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
                delivery_mode=2,  # make message persistent
            ),
            body=json.dumps(data))

        while self.response is None:
            self.connection.process_data_events()
        self.connection.close()
        return self.response


# Worker list api, Lists all present workers.
@app.route("/api/v1/worker/list", methods=["GET"])
def worker_list():
        global client
        container_list = []
        x = client.containers.list()

        # Sort out only worker containers and remove the other containers .
        for i in x:
                print(i.name, i.id)
                if(i.name == "orchestrator" or i.name == "rabbitmq" or i.name == "zookeeper"):
                        print(i.id, i.name)
                else:
                        container_list.append(i.id)
        pid_list = []
        c = docker.APIClient()
        d = 0
        # Using docker sdk low level api, get the pid of the container
        for i in container_list:
                stat = c.inspect_container(i)
                pid_list.append(stat['State']['Pid'])
        pid_list.sort()
        print(pid_list, "sorted list")
        return json.dumps(pid_list)


# Function to initialise the mapping . 
# store the mapping of pid and znode path.
def initialisePidZnodeMapping():
    print("\n\nInitialising pidZnodeMapping . . .")
    global client
    container_list = []
    # get container list, by filtering out using image name
    x = client.containers.list(filters={"ancestor": "master"})
    y = client.containers.list(filters={"ancestor": "slave"})
    container_list.append(x[0].id)
    container_list.append(y[0].id)
    pidList = []
    c = docker.APIClient()
    d = 0
    for i in container_list:
            stat = c.inspect_container(i)
            pidList.append(stat['State']['Pid'])
    pidZnodeMapping[pidList[0]]="/producer/Worker0"
    pidZnodeMapping[pidList[1]]="/producer/Worker1"



# Api to serve crashing of slave containers.
@app.route("/api/v1/crash/slave", methods=["POST"])
def crash_slave():
        global scalingDown
        global crashSlaveApiCalled
        slave_list = []
        crashSlaveApiCalled=True
        mapping = {}

        # Get only slave containers using filtering by image 
        x = client.containers.list(filters={"ancestor": "slave"})
        for i in x:
            slave_list.append(i.id)
        slave_pid = []
        c = docker.APIClient()
        # Get Pid of each container
        for i in slave_list:
            stat = c.inspect_container(i)
            PID = (stat['State']['Pid'])
            if(PID!=currentMasterpid):
                slave_pid.append(PID)
                mapping[PID] = i
        slave_pid.sort()
        print(mapping, "Killing slave")
        if(len(slave_pid) == 0):
            print("no containers to kill")
        # Get the largest PID among containers 
        largest_pid = slave_pid[len(slave_pid)-1]
        to_be_killed = mapping[largest_pid]
        for j in x:
            if(j.id == to_be_killed):
                # Crash the slave by sending stop signal to it
                j.stop()
                exitCode=j.wait()
                print("\nExitcode",exitCode)
                # j.remove()
        return json.dumps(largest_pid)


# Api to serve crashing of master containers.
@app.route("/api/v1/crash/master", methods=["POST"])
def crash_master():
        print("\nkilling master")
        global scalingDown
        global crashSlaveApiCalled
        slave_list = []
        crashSlaveApiCalled=True
        mapping = {}
        # Get the master and slave container pids
        if(currentMasterpid==0):
            x = client.containers.list(filters={"ancestor": "master"})
        else:
            x = client.containers.list(filters={"ancestor": "slave"})
        print(x, "worker containers")
        for i in x:
            slave_list.append(i.id)
        slave_pid = []
        c = docker.APIClient()
        # From the available PIDS extract only the master PID
        if(currentMasterpid==0):
            for i in slave_list:
                stat = c.inspect_container(i)
                PID = (stat['State']['Pid'])
                slave_pid.append(PID)
                mapping[PID] = i
        else:
            for i in slave_list:
                stat = c.inspect_container(i)
                PID = (stat['State']['Pid'])
                if(PID==currentMasterpid):
                    slave_pid.append(PID)
                    mapping[PID] = i
                    
        slave_pid.sort()
        print(mapping, "Killing master")
        if(len(slave_pid) == 0):
            print("no containers to kill")
        largest_pid = slave_pid[len(slave_pid)-1]
        to_be_killed = mapping[largest_pid]
        for j in x:
            if(j.id == to_be_killed):
                # Crash the slave by sending stop signal to it
                j.stop()
                exitCode=j.wait()
                print("\nExitcode",exitCode)
        return json.dumps(largest_pid)


#clear database
@app.route('/api/v1/db/clear',methods=['POST'])
def clear_db():
#    global count
 #   count+=1
    dbaasUrl='127.0.0.1'
    sql_del = {"insert":["username"],"table":"Users","columns":["uname"],"isDelete":"False","isClear":"True"}
    del_ride = ""
    #requests.post(url='http://127.0.0.1:6000/api/v1/db/clear',json=del_ride)    #assuming the other containers port = 6000
    del_rides = {"insert":["username"],"table":"Rides","columns":["uname"],"isDelete":"False","isClear":"True"}
    del_areas = {"insert":["username"],"table":"Areas","columns":["uname"],"isDelete":"False","isClear":"True"}
    del_joined = {"insert":["username"],"table":"joinedRides","columns":["uname"],"isDelete":"False","isClear":"True"}
    try:
        requests.post(url=dbaasUrl+'/api/v1/db/write',json=sql_del)
        requests.post(url=dbaasUrl+'/api/v1/db/write',json=del_rides)
        requests.post(url=dbaasUrl+'/api/v1/db/write',json=del_areas)
        requests.post(url=dbaasUrl+'/api/v1/db/write',json=del_joined)
    except Exception as e:
        print('Db Clear failed')

    return Response(status=200)

# Read DB API , forward the request to slave workers .
@app.route('/api/v1/db/read',methods=["POST"])
def readDB():
    print(pidZnodeMapping)
    global ischeckingReqCount
    # if its the first read request then start a timer , running every 2 min and achieve scalability.
    if(not ischeckingReqCount):
        threading.Thread(target=checkReqCount,args=()).start()
        ischeckingReqCount=True
    global count
    count+=1
    print("\n read Request . . \n")
    result={}
    data = request.get_json()
    readRpc = RpcClient(True,"responseq","readq")
    readResponse=readRpc.call(data)
    print('\n\n Readresponse :',readResponse)
    return jsonify(readResponse)
    

@app.route('/api/v1/db/write',methods=["POST"])
def addToDB():
    print("\n write Request . . \n")
    result={}
    data = request.get_json()
    print(data,"writeRequestData")
    readRpc = RpcClient(False,"writeqResponse","writeq")
    writeResponse=readRpc.call(data)
    print('\n\n WriteResponse :',writeResponse)
    return jsonify(writeResponse)

if __name__ == '__main__':
    app.debug=True
    initialisePidZnodeMapping()
    app.run(host="0.0.0.0",port=80,use_reloader=False)

