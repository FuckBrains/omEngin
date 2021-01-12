import MySQLdb
import os
import string

db = MySQLdb.connect (host="23.152.224.49",
    user="akomi",
    passwd="1q2w3eaz$",
    db="omdb",
    local_infile = 1) #Grants permission to write to db from an input file. Without this you get sql Error: (1148, 'The used command is not allowed with this MySQL version')

print("Connection to DB established")

#The statement 'IGNORE 1 LINES' below makes the Python script ignore first line on csv file
#You can execute the sql below on the mysql bash to test if it works
sqlLoadData = """load data local infile 'i.csv' into table ipasn FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;"""

curs = db.cursor()   
curs.execute(sqlLoadData)
db.commit()   
print("SQL execution complete")
resultSet = curs.fetchall()

print("Data loading complete")