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
count = 0

@app.route("/")
def hello():
    return "<center><h1>Hello Anup !!</h1></center>"

@app.route("/api/v1/users",methods=['POST','HEAD'])
def dummyFun():
    global count
    count+=1
    return Response(status=405)

@app.route("/api/v1/users/check",methods=['GET'])
def check():
    return "<center><h1>Hello Anup ! This is Anup Speaking From User Container !!!</h1></center>"

@app.route("/check",methods=['GET'])
def checking_Communication():
    print("From user Container !")
    return "<h1>Hey , Rides container called me!</h1>"

#1st api
@app.route('/api/v1/users', methods=['PUT'])
def add_user():
    global count
    count+=1
    print('adding user . .  ./n')
    try:
        data = request.get_json()
        uname = data['username']
        pword = data['password']
    except:
        return Response(status=400) 

    query = {"table":"Users","columns":["uname"] ,"where":"1"}

    uname_list = requests.post(url=dbaasUrl+'/api/v1/db/read',json=query)
    valid=1
    
    print("9th api called . . .")
    
    uname_list=uname_list.json()

    print(uname_list)

    

    for x in uname_list["uname"]:
        if(uname==x):
            print("duplicate")
            valid=0
            break

    if(valid):
        #check password
        print("Password is :"+pword)
        match = re.search(pword_pat,pword)
        if not match:
            #invalid password
            return Response(status=400)
        else:
            #add
            sql_add = {"insert":[uname,pword],"table":"Users","columns":["uname","pwd"],"isDelete":"False","isClear":"False"}
            requests.post(url=dbaasUrl+'/api/v1/db/write',json=sql_add)
            return Response(status=201)
    else:
        #invalid uname
        return Response(status=400)



#2nd api
@app.route('/api/v1/users/<username>',methods=['DELETE'])
def del_user(username):
    global count
    count+=1
    query = {"table":"Users","columns":["uname"] ,"where":"1"}

    uname_list = requests.post(url=dbaasUrl+'/api/v1/db/read',json=query)
    present=0
    
    print("9th api called . . .")
    
    uname_list=uname_list.json()

    print(uname_list)
    print(username)
    

    for x in uname_list["uname"]:
        if(username == x):
            print("duplicate")
            present=1
            break
    if(present):
        print("present")
        sql_del = {"insert":[username],"table":"Users","columns":["uname"],"isDelete":"True","isClear":"False"}
        requests.post(url=dbaasUrl+'/api/v1/db/write',json=sql_del)
        return Response(status=200)

    else:
        #user not present
        return Response(status=400)

#list all users
@app.route('/api/v1/users', methods=["GET"])
def list_users():
    global count
    count+=1
    query = {"table":"Users","columns":["uname"] ,"where":"1"}
    print("hi\n")
    uname_list = requests.post(url=dbaasUrl+'/api/v1/db/read',json=query)
    
    unames = uname_list.json()['uname']
    
    print("9th api called . . .")
    
    return json.dumps(unames)

#clear database
@app.route('/api/v1/db/clear',methods=['POST'])
def clear_db():
#    global count
 #   count+=1
    sql_del = {"insert":["username"],"table":"Users","columns":["uname"],"isDelete":"False","isClear":"True"}
    requests.post(url=dbaasUrl+'/api/v1/db/write',json=sql_del)

    del_ride = ""
    #requests.post(url='http://127.0.0.1:6000/api/v1/db/clear',json=del_ride)    #assuming the other containers port = 6000

    return Response(status=200)


@app.route('/api/v1/_count', methods=['GET'])
def get_count():
    try:
        ls = [count]
        return json.dumps(ls)
    except:
        return Response(status=500)

@app.route('/api/v1/_count', methods=['DELETE'])
def set_count():
    try:
        global count
        count = 0
        return "",200
    except:
        return Response(status=500)

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
        sqlQuery = 'DELETE FROM Users'
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