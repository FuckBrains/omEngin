import pandas as pd
import cx_Oracle
import time
import os
from datetime import date
from datetime import datetime
from datetime import timedelta
import win32com.client

pt = os.getcwd() + "\\" + "csv_download\\"
today = date.today()
t = time.localtime()
td = today.strftime('%Y%m')
folderName1 = today.strftime('%m%d%y')
folderName2 = time.strftime("%H%M", t)
pth = os.path.join(pt + folderName1 + folderName2 + '.csv')

def qry_delta(from_min,to_min): 
    tday = date.today()
    tmdlta_from_now = datetime.now() - timedelta('minutes='+ int(from_min))
    tmdlta_to_now = datetime.now() - timedelta('minutes='+ int(to_min))
    qry_from = tmdlta_from_now.strftime('%Y-%m-%d %H:%M:%S')
    qry_to = tmdlta_to_now.strftime('%Y-%m-%d %H:%M:%S')
    dyn_date = "TO_DATE('" + qry_from + "','dd/mm/yyyy hh:mi:ss') AND TO_DATE('" + qry_to + "','dd/mm/yyyy hh:mi:ss')"
    return dyn_date

conn = cx_Oracle.connect('SOC_READ', 'soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
print(conn.version)

single_clk = """Select * from alerts_status where Summary IN ('2G SITE DOWN','3G SITE DOWN','4G SITE DOWN','HUW-MAINS FAILURE','HUW-DC VOLTAGE LOW','ERI-DC LOW VOLTAGE','ERI-AC MAINS FAILURE','ERI-AC MIANS FILT') 
and Summary not like 'Synthetic_Fluc' and Severity>0 and Type=1"""

query = "SELECT * FROM ALERTS_STATUS PARTITION (STATUS_MDA_SEM_DAT_" + td + ") WHERE " + qry_delta(300,0)
print(query)
print(qry_delta('400','0'))
#df = pd.read_sql(query, con=conn)
#print(df)
#df.to_csv(pth)












