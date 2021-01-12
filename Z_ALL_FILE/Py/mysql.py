#ref: https://www.geeksforgeeks.org/mysqldb-connection-python/
import MySQLdb
import os
import pymysql
import pandas as pd
from sqlalchemy import create_engine

engin = create_engine('pymysql+mysqlconnector://' + 'root' + ':' + 'root' + '@' + '127.0.0.1' + ':' + '3306', echo=False)

#try:
    #conn= MySQLdb.connect("localhost","root","root","ops1")
#except:
    #print("Can't connect to database")
#cursor = conn.cursor()

sql = """CREATE TABLE `ops1`.`OMIDB2` ( `SITECODE` VARCHAR(255) NOT NULL , 
`THANA` VARCHAR(255) NULL DEFAULT NULL , `DISTRICT` VARCHAR(255) NULL DEFAULT NULL , 
`REGION` VARCHAR(255) NULL DEFAULT NULL , `LON` DOUBLE NULL DEFAULT NULL , 
`LAT` DOUBLE NULL DEFAULT NULL , `P1P2` VARCHAR(255) NULL DEFAULT NULL , 
`OWNER` VARCHAR(255) NULL DEFAULT NULL , `LINK` VARCHAR(255) NULL DEFAULT NULL , 
`PG` VARCHAR(255) NULL DEFAULT NULL , `PWR_AUT` VARCHAR(255) NULL DEFAULT NULL , 
UNIQUE (`SITECODE`(255))) ENGINE = InnoDB;"""

df = pd.read_csv("F:\\MYDB.csv")
df.to_sql(name='OMIDB2', con=engin, if_exists = 'append', index=False)
#df.to_sql('OMIDB2', con = conn, if_exists = 'append', chunksize = 1000)
#cols = "`,`".join([str(i) for i in df.columns.tolist()])
# Insert DataFrame recrds one by one.
#for i,row in df.iterrows():
    #sql = "INSERT INTO `OMIDB2` (`" + cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    #cursor.execute(sql, tuple(row))
    #conn.commit()
conn.close()
