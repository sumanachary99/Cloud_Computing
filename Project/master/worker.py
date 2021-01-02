#!/usr/bin/env python
import pika
from flask import Flask, request, abort, render_template,jsonify, Response
import sqlite3
import requests
import re
import datetime
import json
import time
import argparse
import threading
import sys
time.sleep(15)
p=sys.argv[1]

# Take command line flags
# Deterimine if this container will run the master or slave code 

if(p=="False"):
    isMaster=False
elif p=="True":
    isMaster=True
print(isMaster)

# Master code:
# Function to syncSlaves to make them consistent with database
# Message is broadcasted from master container to all slaves to keep them updated
def syncSlaves(sqlQuery,channel):
    query={
        "query":sqlQuery
    }
    # Get the query to be broadcasted
    syncQuery=json.dumps(query)

    # Declare a fanout exchange
    channel.exchange_declare(exchange='syncExchange', exchange_type='fanout')
    # Publish the message to the exchange
    channel.basic_publish(exchange='syncExchange', routing_key='', body=syncQuery)

    # To copy Whole database in newly spawned slaves 
    # create a durable queue ,
    # which is appended with all messages which master successfully executes
    try:
        channel.queue_declare(queue='copyDb',durable=True)
    except Exception as e:
        print("\n Failed to create a temp queue for syncing whole db\n")
    try:
        #  Publish the message to the exchange
        channel.basic_publish(exchange='',
                      routing_key='copyDb',
                      body=syncQuery)
    except Exception as e:
        print("\n Failed to append query to copyDb queue for syncing whole db\n")
    print(" [x] Sent " , syncQuery)



# A function to execute a sql query on to the database
# Takes a sql query as argument and executes it
def executeSqlQuery(body):
        data = json.loads(body)
        result={}
        result['status']=200
        query=data['query']
        try:
            # connect to database
            cxn=sqlite3.connect('rideshare.db')
            cursor=cxn.cursor()
            cursor.execute('PRAGMA foreign_keys = ON')
        except Exception as e:
            cxn.close()
            result['status']=400
            print(e)
            return result
        cxn.commit()
        try:
            # Execute the query
            cursor.execute(query)
            cxn.commit()
        except Exception as e:
            print("sql write error:",e)
            cxn.close()
            result['status']=400
            print(e)
            return result
        cxn.close()
        return result


# A callback function ,
#  executed when a message is broadcasted from master to sync the data
def callback(ch, method, properties, body):
        print(" [x] in slave recieved sqlQuery to be executed :",  body)
        # Execute the sql query
        result=executeSqlQuery(body)
        print(result,"result of sync in slave")
        # Acknowkledge the message that it has successfully processed
        ch.basic_ack(delivery_tag = method.delivery_tag)
        return result

# This function provides slaves to be consistent
# continuously listens on the queue to consume any new messages to be consistent always.
def trySyncIfAny(channel):
    try:
        channel.exchange_declare(exchange='syncExchange', exchange_type='fanout')
    except Exception as e:
        print(e)
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    # Bind the queue to the fanout exchange
    channel.queue_bind(exchange='syncExchange', queue=queue_name)

    print(' [*] Waiting for logs. To exit press CTRL+C')
    # Consume the message once it arrives and call the function "callback"
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback)

    
# this function writes to db. 
# If the function is called to copyTheDB , then the query is not sent to fanout exchange.
# Else the message is broadcasted to all the slaves
def writeToDb(body,channel,isCopyDbOperation):
    print('\n\nwriting db..\n\n')
    result={}
    result['status']=200
    try:
        # connect to database
        cxn=sqlite3.connect('rideshare.db')
        cursor=cxn.cursor()
        cursor.execute('PRAGMA foreign_keys = ON')
    except Exception as e:
        cxn.close()
        result['status']=400
        print(e)
        return result
    cxn.commit()
    data = json.loads(body)
    # Get all the data from the arguments.
    isDelete=data['isDelete']
    isClear = data['isClear']
    sqlQuery=""
    print(isDelete)
    tableName=data['table']
    insertData=data['insert']
    columns=data['columns']
    # if cleardb operation then execute this
    if(isClear=="True"):
        sqlQuery = 'DELETE FROM Users'

    # if Delete operation then execute this
    elif isDelete=="True":
        sqlQuery='DELETE FROM '+tableName+ ' WHERE '+columns[0]+'="'+insertData[0]+'"'
    # else just execute a insert query
    else:
        
        sqlQuery='INSERT INTO '+tableName + ' ('
        for i in columns:
            sqlQuery=sqlQuery+i+','
        sqlQuery=sqlQuery[0:-1]
        sqlQuery=sqlQuery+') VALUES('

        for i in insertData:
            sqlQuery+='"'+i+'"'+','
        sqlQuery=sqlQuery[0:-1]
        sqlQuery+=')'
        print("\n\n"+sqlQuery)
        
    try:
        cursor.execute(sqlQuery)
        cxn.commit()
    except Exception as e:
        cxn.close()
        result['status']=400
        return result
    cxn.close()
    if(not isCopyDbOperation):
        # Send the executed sql query into the fanout exchange tosync slaves
        syncSlaves(sqlQuery,channel)
    print(result)
    return result


def readFromDb(body):
    print("reading DB. . .")
    result={}
    try:
         # connect to database
        cxn=sqlite3.connect('rideshare.db')
        cursor=cxn.cursor()
        cursor.execute('PRAGMA foreign_keys = ON')
        print("Connected")
        
    except Exception as e:
        cxn.close()
        result['status']=400
        print(result)
        return jsonify(result)


    cxn.commit()
    data = json.loads(body)
    sqlQuery=""
     # Get all the data from the arguments.
    tableName=data['table']
    whereClause=data['where']
    columns=data['columns']
    sqlQuery='SELECT '
    for i in columns:
        sqlQuery+=i+','
    sqlQuery=sqlQuery[0:-1]
    sqlQuery+=' FROM '+tableName + ' WHERE '+whereClause
    
    print(sqlQuery)
    
    try:
        # Execute the SQL query
        cursor.execute(sqlQuery)
        rows = cursor.fetchall()
        result["count"]=len(rows)
        result["status"]=200
        k=-1
        for i in columns:
            result[i]=[]
            k+=1
            for data in rows:
                result[i].append(data[k])
        cxn.commit()
    except Exception as e:
        cxn.close()
        result['status']=400
        return result
    cxn.close()
    return result
# Function called whenever a message is read from the queue.
# All these recieved queries are executed one by one , hence the worker becomes consistent.
def copyDbCallback( body):
        print(" [x] Received while copying whole db%r" % body)
        executeSqlQuery(body)


# Function to copy whole database initially for a fresh container
def copyWholeDbInSlave(channel):
    try:
        declareStatus = channel.queue_declare(queue="copyDb", durable=True)
    except Exception as e:
        print(e,"exception")
        print("\n Failed to declare a copyDb queue for syncing whole db in slave\n")
    try:
        noOfMsg=declareStatus.method.message_count
        while(declareStatus.method.message_count!=0):
            messageRes = channel.basic_get(queue='copyDb',auto_ack=False)
            copyDbCallback(messageRes[2])
            declareStatus = channel.queue_declare(queue="copyDb", durable=True)
    except Exception as e:
        print("\n Failed to read all messages in the copyDb queue while syncing whole db in slave\n")
    print("Slave consistent now with master!")
    channel.close()

# Execute this code if this container has to become master
if(isMaster):
    # Connect to rabbitMq server
    connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    # declare relevant queues
    channel.queue_declare(queue='writeq',durable=True)
    try:
        channel.queue_declare(queue='copyDb',durable=True)
    except Exception as e:
        print("\n Failed to create a temp queue for syncing whole db\n")
    # this function called whenever a write request is recieved from the queue
    def on_request(ch, method, props, body):
        response = writeToDb(body,channel,False)
        ch.basic_publish(exchange='',
                        routing_key=props.reply_to,
                        properties=pika.BasicProperties(correlation_id = props.correlation_id),
                        body=json.dumps(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)
    # Specify that the messages should be delivered to a worker only if it has already returned the ack of previous message
    channel.basic_qos(prefetch_count=1)
    # Get message from the writeQ to respond to write Requests
    channel.basic_consume(queue='writeq', on_message_callback=on_request)

    print("\n\n [x] Awaiting RPC requests in master\n\n")
    channel.start_consuming()


    
# Execute this code if this container has to become slave
else:
    # Connect to rabbitmq server
    connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel1 = connection.channel()
    copyDbChannel=connection.channel()
    channel.queue_declare(queue='readq',durable=True)
    
    # this function called whenever a read request is recieved from the queue
    def on_request(ch, method, props, body):
        # Get the response to the read request
        response = readFromDb(body)
        # Send the response back to orchestrator using RPC
        ch.basic_publish(exchange='',
                        routing_key=props.reply_to,
                        properties=pika.BasicProperties(correlation_id = props.correlation_id),
                        body=json.dumps(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Specify that the messages should be delivered to a worker only if it has already returned the ack of previous message
    channel.basic_qos(prefetch_count=1)
    
    # Before starting consuming read request , first the slave has to copy whole database from the queue.
    copyWholeDbInSlave(copyDbChannel)

    # After whole DB is copied
    # Read requests are started consuming from readq
    channel.basic_consume(queue='readq', on_message_callback=on_request)
    
    print("\n\n [x] Awaiting RPC requests in Slave\n\n")
    print("creating thread for sync in slave")
    
    channel.exchange_declare(exchange='syncExchange', exchange_type='fanout')
    # print(e)
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='syncExchange', queue=queue_name)

    print(' [*] Waiting for logs. To exit press CTRL+C')
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback)
    # threading.Thread(target=trySyncIfAny,args=(channel,)).start()
    channel.start_consuming()
    print("\n\n [x] Started consuming . .in slave \n\n")