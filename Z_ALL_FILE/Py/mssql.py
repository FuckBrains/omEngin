import pyodbc
import pandas as pd

UserEx = "Driver={SQL Server};Server=10.101.4.193;Database=ROC;Uid=om29861;Pwd=Roc@072$123"
UserRd = "Driver={SQL Server};Server=10.101.4.193;Database=ROC;Uid=rocuser;Pwd=Roc@072$123"
UserSMS = "Driver={SQL Server};Server=10.101.4.193;Database=ROC;Uid=om29861;Pwd=Roc@072$123"
socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"

conn = pyodbc.connect(socdb)
df = pd.read_csv("F:\\MYDB.csv",delimiter = ',')
cursor = conn.cursor()
for index, row in df.iterrows():
	#print(row)
	cursor.execute("INSERT INTO 'omidb' ([SITECODE],[THANA],[DISTRICT],[REGION],[LON],[LAT],[P1P2],[OWNER],[LINK],[PG],[PWR_AUT]) values (?,?,?,?,?,?,?,?,?,?,?)",
    (row['SITECODE'], row['THANA'], row['DISTRICT'], row['REGION'], row['LON'], row['LAT'], row['P1P2'], row['OWNER'], row['LINK'], row['PG'], row['PWR_AUT']))
conn.commit()

cursor.close()
conn.close()



#df.to_sql(name = "omidb",con = conn, if_exists = 'append', chunksize = 100000)
#cursor.execute(sql)  # table created
#conn.close()