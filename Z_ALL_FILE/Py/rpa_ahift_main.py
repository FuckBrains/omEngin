import pandas as pd
import cx_Oracle
import time
import os
from datetime import date
import win32com.client
import rpa_shift.omdt as odt

start = time.time()

t = time.localtime()
today = date.today()
f1 = today.strftime('%m%d%y')
f2 = time.strftime("%H%M", t)
pt = os.getcwd() + "\\T\\" + f1 + f2 + '.csv'


YM1 = today.strftime('%Y%m')
YMT = odt.deltamonth(odt.nw(),-1)
YM2 = YMT.strftime("%Y%m")

#selcol = ' * '
selcol = """ SERIAL,SEVERITY,NODE,EQUIPMENTKEY,FIRSTOCCURRENCE,LASTOCCURRENCE,TALLY,CUSTOMATTR11,SUMMARY,CUSTOMATTR26,POSSIBLEROOTCAUSE,PARENTPOINTER,CUSTOMATTR23,RCASTATUS,TTSEQUENCE,TTSTATUS,CUSTOMATTR15,CUSTOMATTR24,ALERTKEY,RCAPARENTHISTORY,SUPPRESSESCL,TTFLAG,TTREQUESTTIME,CUSTOMATTR3,INHAND,OWNERGID,AGENT,MANAGER,EVENTID,INHANDEXPIRETIME,TTREQUESTTIME,CUSTOMATTR19,CLEARTIMESTAMP,SRCEMSIDENTIFIER,RCATIMESTAMP,RCATALLY,ADVCORRSERVERSERIAL,ADVCORRCAUSETYPE,ROOTCAUSEDESC,ACFLAG,CLEAREDBY """
qst1_1 = 'Select' + selcol + 'from ALERTS_STATUS PARTITION (STATUS_MDA_SEM_DAT_' + YM1 + ') '
qst2_2 = 'Select' + selcol + 'from ALERTS_STATUS PARTITION (STATUS_MDA_SEM_DAT_' + YM2 + ') '

conn = cx_Oracle.connect('SEMHEDB', 'SEMHEDB', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
print(conn.version)

Code = "('DHMRP25','CGPCH19')"
P1 = "Select * from alerts_status where Summary IN " + Code
P2 = "Select" + selcol + "from alerts_status where"
Q1 = " and (TO_DATE(CLEARTIMESTAMP,'DD-MM-RRRR')='01-JAN-1970')"
Q2 = " and Severity>0 and Type=1"
Q3 = " Severity>0 and Type=1"
Qr1 = P2 + Q3
print(Qr1)
df = pd.read_sql(Qr1, con=conn)
end = time.time()
print('TIme Required: ')
print(end - start)
df.to_csv(pt)
print("file name: " + pt)