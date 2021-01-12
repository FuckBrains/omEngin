import pandas as pd
import cx_Oracle
import time
import os
from datetime import date
import win32com.client
import omdt as odt
import subprocess
import xlwings

print("RPA is Runnig in BackGround, Don't CLose")
scrpt_name = "ErrorHanddle_VBS.vbs"
#fpth_0 = os.getcwd() + "\\" + scrpt_name
#os.system(fpth_0)
#time.sleep( 3 )

pt = os.getcwd()
pt2 = pt + "\\download\\"
t = time.localtime()
today = date.today()
folderName1 = today.strftime('%m%d%y')
folderName2 = time.strftime("%H%M", t)
pth = os.path.join(pt2 + folderName1 + folderName2 + '.csv')
pth2 = os.path.join(pt2 + folderName1 + '.csv')
print(pth)
conn = cx_Oracle.connect('SEMHEDB', 'SEMHEDB', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
print(conn.version)

def timex():
    t = time.localtime()
    folderName2 = time.strftime("%H%M", t)
    return folderName2

selcol = """ SERIAL,SEVERITY,NODE,EQUIPMENTKEY,FIRSTOCCURRENCE,LASTOCCURRENCE,TALLY,CUSTOMATTR11,SUMMARY,CUSTOMATTR26,POSSIBLEROOTCAUSE,PARENTPOINTER,CUSTOMATTR23,RCASTATUS,TTSEQUENCE,TTSTATUS,CUSTOMATTR15,CUSTOMATTR24,ALERTKEY,RCAPARENTHISTORY,SUPPRESSESCL,TTFLAG,TTREQUESTTIME,CUSTOMATTR3,INHAND,OWNERGID,AGENT,MANAGER,EVENTID,INHANDEXPIRETIME,TTREQUESTTIME,CUSTOMATTR19,CLEARTIMESTAMP,SRCEMSIDENTIFIER,RCATIMESTAMP,
            RCATALLY,ADVCORRSERVERSERIAL,ADVCORRCAUSETYPE,ROOTCAUSEDESC,ACFLAG,CLEAREDBY """
qstr4 = "WHERE Severity>0 and Type=1"
#qstr4 = "WHERE (TO_DATE(CLEARTIMESTAMP,'DD-MM-RRRR')='01-JAN-1970')"
YM1 = today.strftime('%Y%m')
YMT = odt.deltamonth(odt.nw(),-1)
YM2 = YMT.strftime("%Y%m")
qst1_1 = 'Select' + "*" + 'from ALERTS_STATUS PARTITION (STATUS_MDA_SEM_DAT_' + YM1 + ') '
qst2_2 = 'Select' + selcol + 'from ALERTS_STATUS PARTITION (STATUS_MDA_SEM_DAT_' + YM2 + ') '
qry_un1 = qst1_1 + qstr4
qry_un2 = qst2_2 + qstr4
tm1 = timex()
df = pd.read_sql(qry_un1, con=conn)
tm2 = timex()
print("downloaded")
df.to_csv(pth2)