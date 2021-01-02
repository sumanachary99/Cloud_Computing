import csv, sqlite3

con = sqlite3.connect("rideshare.db")
cur = con.cursor()

users = "CREATE TABLE Users (uname NVARCHAR(50)  NOT NULL PRIMARY KEY, pwd NVARCHAR(40)  NOT NULL  );"
rides = "CREATE TABLE Rides ( rideID INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,source INTEGER NOT NULL,destination INTEGER NOT NULL,uname NVARCHAR(50) NOT NULL,CONSTRAINT fk_users	FOREIGN KEY (uname) REFERENCES Users(uname) ON DELETE CASCADE);"
joinedrides = "CREATE TABLE joinedRides( rideID INTEGER  NOT NULL,uname NVARCHAR(50) NOT NULL,PRIMARY KEY (rideID,uname),FOREIGN KEY (uname) REFERENCES Users(uname) ON DELETE CASCADE , FOREIGN KEY (rideId) REFERENCES Rides(rideId) ON DELETE CASCADE);"
areas = "CREATE TABLE areas (area_id INTEGER NOT NULL PRIMARY KEY, name NVARCHAR(20) NOT NULL);"

cur.execute(users)
cur.execute(rides)
cur.execute(joinedrides)
cur.execute(areas)

with open('AreaNameEnum.csv','r', encoding='utf8') as fin: # `with` statement available in 2.5+
    # csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['area_id'], i['name']) for i in dr]

cur.executemany("INSERT INTO areas (area_id, name) VALUES (?, ?);", to_db)
con.commit()
con.close()
