import sqlite3
from flask import Flask, render_template,jsonify,request,abort,Response
import requests
import json
import csv
import pika
import time
import uuid
import docker
client=docker.from_env()
def worker_list():
        global client
        container_list=[]
        x=client.containers.list()
        print(x,"containers")
        print(x[0])
        for i in x:
                print(i.name,i.id)
                if(i.name=="orchestrator" or i.name=="rabbitmq"):
                        print(i.id,i.name)
                else:
                        container_list.append(i.id)
        pid_list=[]
        c=docker.APIClient()
        # print(c,"\nApiClient\n")
        # print(container_list[0],"env\n")
        # temp=c.inspect_container(container_list[0])
        # print(temp['Config']['Env'][6],'env\n')
        # temp['Config']['Env'][6]="master"
        # print(temp['Config']['Env'][6],'changedEnv\n')
        # c=docker.APIClient()
        d=0
        for i in container_list:
                stat=c.inspect_container(i)
                pid_list.append(stat['State']['Pid'])
                # if d==0:
                #   print(stat,"\nStat\n")
                #   print(stat['State'],"\nState\n")
                #   d=1
        pid_list.sort()
        print(pid_list,"sorted list")
        return json.dumps(pid_list)

worker_list()