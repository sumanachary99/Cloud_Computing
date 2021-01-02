from flask import Flask, request, abort, render_template,jsonify, Response
import sqlite3
import requests
import re
import datetime
import json
pword_pat = re.compile('^[a-fA-F0-9]{40}$')
app = Flask(__name__)
ridesUrl=""
dbaasUrl='http://34.235.3.103'
count=0

@app.route("/")
def hello():
    return "<h1>Hello Anup</h1>"

@app.route("/api/v1/rides",methods=['PUT'])
def dummyFun():
    global count
    count+=1
    return Response(status=405)


@app.route("/communicate",methods=['GET'])
def callUser():
    returnVal = requests.get(url='http://ec2-54-175-126-172.compute-1.amazonaws.com:80/check')
    print("From ride container after calling user container")
    return "called user container"

#3rd api
@app.route('/api/v1/rides',methods=['POST'])
def create_ride():
    global count
    count+=1
    print("Creating RIde . . .")
    try:
        data = request.get_json()
        created_by = data['created_by']
        timestamp = data['timestamp']
        source = data['source']
        dest = data['destination']
    except:
        return Response(status=400)

    query = {}
    try:
        header = {'Origin': 'http://34.196.38.115'}
        uname_list = requests.get(url='http://ridesharelb-1246004696.us-east-1.elb.amazonaws.com:80/api/v1/users',params=query)
    except:
        return Response(status=400)
    present=0
    uname_list=uname_list.json()
    print(uname_list)
    for x in uname_list:
        if(created_by==x):
            present=1
            break
    if(present):
        send_data = {"insert":[created_by,timestamp,source,dest],"table":"rides","columns":["uname","timestamp","source","destination"],"isDelete":"False","isClear":"False"}
        try:
            requests.post(url=dbaasUrl+'/api/v1/db/write', json=send_data)
            return Response(status=201)
        except:
            return Response(status=400)

    else:
        #user who created ride not present
        return Response(status=400)


#4th api
@app.route('/api/v1/rides',methods=['GET'])
def list_rides():
    global count
    count+=1
    try:
        source = request.args.get('source')
        destination = request.args.get('destination')
    except:
        return Response(status=400)


    src = 0
    dest = 0
    query_for_area = {"table":"Areas","columns":["area_id"], "where":"1"}

    try:
        area_list = requests.post(url=dbaasUrl+'/api/v1/db/read',json = query_for_area)
    except:
        return Response(status=400)
    area_list = area_list.json()['area_id']

    #print(type(source))

    for x in area_list:
        if(x==int(source)):
            src=1
        elif(x==int(destination)):
            dest = 1
    print(src , dest)
    if(src and dest):
        condition = "source="+str(source)+" and destination="+str(destination)
        query = {"table":"rides","columns":["rideId","uname","timestamp"],"where":condition} 

        try:
            ride_data = requests.post(url=dbaasUrl+'/api/v1/db/read', json=query)
        except:
            return Response(status=400)
        print(ride_data)

        ride_data = ride_data.json()
        response = []
        
        print(ride_data)

        for x in range(ride_data['count']):
            timestamp1 = ride_data['timestamp'][x]
            timestamp2 = datetime.datetime.now().strftime("%d-%m-%Y:%S-%M-%H")

            t1 = datetime.datetime.strptime(str(timestamp1), "%d-%m-%Y:%S-%M-%H")
            t2 = datetime.datetime.strptime(str(timestamp2), "%d-%m-%Y:%S-%M-%H")

            print(t1,t2)
            
            if(t1 > t2):
                #list it
                ds = {}
                ds["rideId"] = ride_data['rideId'][x]
                ds["username"] = ride_data['uname'][x]
                ds["timestamp"] = ride_data['timestamp'][x]
                response.append(ds)
        print(response)
        if(len(response)==0):
            return Response(status=204)
        return jsonify(response), 200   
    else:
        print('invlaid src and destination')
        abort(400)


#5th api
@app.route('/api/v1/rides/<rideId>',methods=['GET'])
def ride_details(rideId):
    global count
    count+=1
    print("rideId:",rideId)

    query = {"table":"Rides","columns":["rideId"],"where":"1"}
    try:
        ride_ids = requests.post(url=dbaasUrl+'/api/v1/db/read', json=query)
    except:
        return Response(status=400)

    ride_ids = ride_ids.json()['rideId']
    print("ride_ids:",ride_ids)

    valid = 0
    for x in ride_ids:
        if(x == int(rideId)):
            valid=1
            break
    print(valid)
    if not valid:
        #invalid rided id
        return Response(status=405)
    else:
        wherecond = "rideId="+str(rideId)
        ride_query = {"table":"rides","columns":["uname","timestamp","source","destination"],"where":wherecond} 
        
        try:
            about_ride = requests.post(url=dbaasUrl+'/api/v1/db/read', json=ride_query)
        except:
            return Response(status=400)


        about_ride = about_ride.json()
        print(about_ride)

        created_user = about_ride['uname']
        created_timestamp = about_ride['timestamp']
        source_id = about_ride['source']
        dest_id = about_ride['destination']

        users_query = {"table":"joinedRides","columns":["uname"],"where":wherecond} 

        print(users_query)

        pool_users = requests.post(url=dbaasUrl+'/api/v1/db/read', json=users_query)
        pool_users = pool_users.json()['uname']
        response = {"rideId":rideId, "created_by":created_user,"users":pool_users , "timestamp":created_timestamp, "source":source_id, "destination":dest_id}
        return jsonify(response),200


#6th api
@app.route('/api/v1/rides/<rideId>',methods=['POST'])
def pool_ride(rideId):
    global count
    count+=1
    uname = request.get_json()['username']
    query1 = {}
    header = {'Origin': 'http://34.196.38.115'}
    uname_list = requests.get(url='http://ridesharelb-1246004696.us-east-1.elb.amazonaws.com:80/api/v1/users',params=query1)
    present=0
    
    uname_list=uname_list.json()    

    for x in uname_list["uname"]:
        if(uname==x):
            print("duplicate")
            present=1
            break

    query2 = {"table":"Rides","columns":["rideId"],"where":"1"}
    ride_ids = requests.post(url=dbaasUrl+'/api/v1/db/read', json=query2)
    ride_ids = ride_ids.json()['rideId']
    print("ride_ids:",ride_ids)

    valid = 0
    for x in ride_ids:
        if(x == int(rideId)):
            valid=1
            break

    if((not valid) or (not present)):
        #invalid rided id or username
        abort(405)

    else:
        insert_query = {"insert":[rideId,uname],"table":"joinedRides","columns":["rideID","uname"],"isDelete":"False"}
        added = requests.post(url=dbaasUrl+'/api/v1/db/write', json=insert_query)
        added = added.json()
        print(added)
        if(added['status'] == 200):
            return Response(status=200)   
        else:
            abort(400)


#7th api
@app.route('/api/v1/rides/<rideId>', methods=['DELETE'])
def delete_ride(rideId):
    global count
    count+=1
    query = {"table":"rides","columns":["rideID"],"where":"1"}
    ride_ids = requests.post(dbaasUrl+'/api/v1/db/read',json=query)
    valid = 0
    ride_ids=ride_ids.json()
    
    for x in ride_ids["rideID"]:
        # print(type(x),end=" ")
        # print(type(rideId))
        if(x == int(rideId)):
            valid=1
            break
    
    
    if valid:
        #invalid rided id 
        # delete_sql = "DELETE FROM rides WHERE rideId="+str(rideId)
        # deleted = cursor.execute(delete_sql)
        # if(deleted):
        #     return 200
        # else:
        #     abort(400) '
        print("deleting . . ")
        send_data = {"insert":[rideId],"table":"rides","columns":["rideID"],"isDelete":"True"}
        try:
            requests.post(url=dbaasUrl+'/api/v1/db/write', json=send_data)
            return Response(status=200)
        except:
            return Response(status=405)

    else:
        print("No Ride Exists")
        return Response(status=405)
        

#clear database
@app.route('/api/v1/db/clear',methods=['POST'])
def clear_db():
#    global count
 #   count+=1
    del_rides = {"insert":["username"],"table":"Rides","columns":["uname"],"isDelete":"False","isClear":"True"}
    del_areas = {"insert":["username"],"table":"Areas","columns":["uname"],"isDelete":"False","isClear":"True"}
    del_joined = {"insert":["username"],"table":"joinedRides","columns":["uname"],"isDelete":"False","isClear":"True"}
    requests.post(url=dbaasUrl+'/api/v1/db/write',json=del_rides)
    requests.post(url=dbaasUrl+'/api/v1/db/write',json=del_areas)
    requests.post(url=dbaasUrl+'/api/v1/db/write',json=del_joined)

    return Response(status=200)

@app.route('/api/v1/_count', methods=['GET'])
def get_count():
    try:
        global count
        ls = [count]
        return json.dumps(ls),200
    except:
        return Response(status=405)

@app.route('/api/v1/_count', methods=['DELETE'])
def set_count():
    try:
        global count
        count = 0
        return Response(status=200)
    except:
        return Response(status=405)

@app.route('/api/v1/rides/count', methods=['GET'])
def ride_count():
    global count
    count+=1
    try:
        query = {"table":"rides","columns":["rideID"],"where":"1"}
        ride_ids = requests.post(dbaasUrl+'/api/v1/db/read',json=query)
        valid = 0
        ride_ids=ride_ids.json()
        ride_count = [ride_ids["count"]]
        return json.dumps(ride_count),200
    except:
        return Response(status=405)
    

#8th api
@app.route('/api/v1/db/write',methods=["POST"])
def addToDB():
    result={}
    result['status']=200
    try:
        cxn=sqlite3.connect('rideshare.db')
        cursor=cxn.cursor()
        cursor.execute('PRAGMA foreign_keys = ON')
    except Exception as e:
        cxn.close()
        result['status']=400
        print(e)
        return result
    cxn.commit()
    data=request.get_json()
    print(data)
    
    isDelete=data['isDelete']
    isClear = data['isClear']
    
    sqlQuery=""
    print(isDelete)
    tableName=data['table']
    insertData=data['insert']
    columns=data['columns']

    if(isClear=="True"):
        sqlQuery = 'DELETE FROM '+tableName
        print(sqlQuery)

    elif isDelete=="True":
        print("ELSE")
        sqlQuery='DELETE FROM '+tableName+ ' WHERE '+columns[0]+'="'+insertData[0]+'"'
        print(sqlQuery)
    else:
        print("HI")
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
        print("sql write error:",e)
        cxn.close()
        result['status']=400
        print(e)
        return result
    cxn.close()
    print(result)
    return jsonify(result)


#9th api
@app.route('/api/v1/db/read',methods=["POST"])
def readDB():
    print("reading DB. . .")
    result={}

    try:
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
    data = request.get_json()
    print(data)

    sqlQuery=""
    tableName=data['table']
    whereClause=data['where']
    columns=data['columns']
    
    
    print("HI")
    sqlQuery='SELECT '
    for i in columns:
        sqlQuery+=i+','
    sqlQuery=sqlQuery[0:-1]
    sqlQuery+=' FROM '+tableName + ' WHERE '+whereClause
    
    print(sqlQuery)
    
    try:
        print("abc2")
        cursor.execute(sqlQuery)
        print("abc23")
        rows = cursor.fetchall()
        print("abc24")
 

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
        print("abc")
        cxn.close()
        print(e)
        result['status']=400
        print(result)
        return result
    cxn.close()
    print(result)
    return jsonify(result)


if __name__ == '__main__':
    app.debug=True
    app.run(host="0.0.0.0",port=80)