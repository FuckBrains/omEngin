import pandas as pd
import cx_Oracle
import time
import os
from datetime import date
import win32com.client
import omdt as odt
import subprocess

pt = os.getcwd()
pt2 = pt + "\\"
ptxls = pt + "\\xlsF\\RW.xlsm"
ptbat = pt + "\\closexl.vbs"
subprocess.call("cscript closexl.vbs")
t = time.localtime()
today = date.today()
folderName1 = today.strftime('%m%d%y')
folderName2 = time.strftime("%H%M", t)
pth = os.path.join(pt2 + folderName1 + folderName2 + '.csv')
print(pth)
print(conn.version)

def timex():
    t = time.localtime()
    folderName2 = time.strftime("%H%M", t)
    return folderName2

selcol = """ SERIAL,SEVERITY,NODE,EQUIPMENTKEY,FIRSTOCCURRENCE,LASTOCCURRENCE,TALLY,CUSTOMATTR11,SUMMARY,CUSTOMATTR26,POSSIBLEROOTCAUSE,PARENTPOINTER,CUSTOMATTR23,RCASTATUS,TTSEQUENCE,TTSTATUS,CUSTOMATTR15,CUSTOMATTR24,ALERTKEY,RCAPARENTHISTORY,SUPPRESSESCL,TTFLAG,TTREQUESTTIME,CUSTOMATTR3,INHAND,OWNERGID,AGENT,MANAGER,EVENTID,INHANDEXPIRETIME,TTREQUESTTIME,CUSTOMATTR19,CLEARTIMESTAMP,SRCEMSIDENTIFIER,RCATIMESTAMP,
            RCATALLY,ADVCORRSERVERSERIAL,ADVCORRCAUSETYPE,ROOTCAUSEDESC,ACFLAG,CLEAREDBY """
qstr4 = "WHERE Severity>0 and Type=1"
YM1 = today.strftime('%Y%m')
YMT = odt.deltamonth(odt.nw(),-1)
YM2 = YMT.strftime("%Y%m")
qst1_1 = 'Select' + selcol + 'from ALERTS_STATUS PARTITION (STATUS_MDA_SEM_DAT_' + YM1 + ') '
qst2_2 = 'Select' + selcol + 'from ALERTS_STATUS PARTITION (STATUS_MDA_SEM_DAT_' + YM2 + ') '
qry_un1 = qst1_1 + qstr4
qry_un2 = qst2_2 + qstr4
tm1 = timex()
df1 = pd.read_sql(qry_un1, con=conn)
tm2 = timex()
df2 = pd.read_sql(qry_un2, con=conn)
tm3 = timex()
print('execution start, mid, end: ' + tm1 + ',' + tm2 + ',' + tm3)
conn.close()
df3 = [df1,df2]
df = pd.concat(df3)
df.to_csv("F://Python//RPA_SHIFT//TestData//testdata.csv")
df2g = df[df['SUMMARY'].str.contains('2G SITE DOWN')]
df3g = df[df['SUMMARY'].str.contains('3G SITE DOWN')]
df4g = df[df['SUMMARY'].str.contains('4G SITE DOWN')]
dfmf = df[df['SUMMARY'].str.contains('MAIN')]
dfdl = df[df['SUMMARY'].str.contains('DC LOW')]
df_cnct = [df2g,df3g,df4g,dfmf,dfdl]
df_all = pd.concat(df_cnct)
df_all.to_csv(pth)
df_final = df_all.rename(columns={'EQUIPMENTKEY':'Resource','CUSTOMATTR26':'AssociatedCR',
                                    'CUSTOMATTR24':'BCCH',
                                    'OWNERGID':'Incident Owner',
                                    'EVENTID':'Frequency',
                                    'TTREQUESTTIME':'TT Creation Time',
                                    'CUSTOMATTR19':'HVC_STATUS'})

print(df_final.columns)
#df_final.to_csv(pth)
parm = pth
#xl = win32com.client.Dispatch("Excel.Application")
#xl.Visible = False
#book = xl.Workbooks.Open(ptxls, False, False, None, '2986')
#xl.Application.Run("RW.xlsm!init", parm) #With Parameter
#time.sleep( 10 )
#subprocess.call("cscript closexl.vbs")
print('all sucess with python')
#xl.Application.Quit()
time.sleep( 3 )