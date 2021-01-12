import MySQLdb
import pandas as pd
try:
    conn= MySQLdb.connect("localhost","root","root","ops1")
except:
    print("Can't connect to database")
#cursor = conn.cursor()

df = pd.read_csv("F:\\MYDB.csv",delimiter = ',')
df.to_sql('OMIDB2', conn, if_exists = 'append', index = False)