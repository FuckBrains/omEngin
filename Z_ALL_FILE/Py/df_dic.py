import MySQLdb
import pandas as pd
try:
    conn= MySQLdb.connect("localhost","root","admin","omdb")
except:
    print("Can't connect to database")
#cursor = conn.cursor()








#filename = os.getcwd() + '//inc.csv'
#df = pd.read_csv(filename)
##dic = df.to_dict()