import pandas as pd
import cx_Oracle
import time
import os
from datetime import date
import win32com.client

pt = os.getcwd() + '//T.csv'
conn = cx_Oracle.connect('SEMHEDB', 'SEMHEDB', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
Qr1 = "SELECT IDENTIFIER,CUSTOMATTR15,SUMMARY,LASTOCCURRENCE FROM alerts_status WHERE Severity!='0'"
print(Qr1)
df = pd.read_sql(Qr1, con=conn)
end = time.time()
print('TIme Required: ')
print(end - start)
df.to_csv(pt)
print("file name: " + pt)