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
qstr4 = "WHERE Severity>0"
YM1 = today.strftime('%Y%m')
YMT = odt.deltamonth(odt.nw(),-1)
YM2 = YMT.strftime("%Y%m")
qst1_1 = 'Select' + selcol + 'from ALERTS_STATUS PARTITION (STATUS_MDA_SEM_DAT_' + YM1 + ') '
qst2_2 = 'Select' + selcol + 'from ALERTS_STATUS PARTITION (STATUS_MDA_SEM_DAT_' + YM2 + ') '
qry_un1 = qst1_1 + qstr4
qry_un2 = qst2_2 + qstr4
tm1 = timex()
df = pd.read_sql(qry_un1, con=conn)
tm2 = timex()
print("downloaded")
#df2 = pd.read_sql(qry_un2, con=conn)
#tm3 = timex()
#print('execution start, mid, end: ' + tm1 + ',' + tm2 + ',' + tm3)
#conn.close()
#df3 = [df1,df2]
#df = pd.concat(df3)
df.to_csv(pth2)
df2g = df[df['SUMMARY'].str.contains('2G SITE DOWN', na=False)]
df3g = df[df['SUMMARY'].str.contains('3G SITE DOWN', na=False)]
df4g = df[df['SUMMARY'].str.contains('4G SITE DOWN', na=False)]
dfmf = df[df['SUMMARY'].str.contains('MAIN', na=False)]
dfdl = df[df['SUMMARY'].str.contains('DC LOW', na=False)]
dftmp = df[df['SUMMARY'].str.contains('TEMP', na=False)]
dfcell = df[df['SUMMARY'].str.contains('CELL DOWN', na=False)]
dfth = df[df['SUMMARY'].str.contains('ERI-RRU THEFT', na=False)]
dfsmoke = df[df['SUMMARY'].str.contains('SMOKE ALARM', na=False)]
df_cnct = [df2g,df3g,df4g,dfmf,dfdl,dftmp,dfcell,dfth,dfsmoke]
df_all = pd.concat(df_cnct)
df_final = df_all.rename(columns={'EQUIPMENTKEY':'Resource','CUSTOMATTR26':'AssociatedCR',
                                    'CUSTOMATTR24':'BCCH',
                                    'OWNERGID':'Incident Owner',
                                    'EVENTID':'Frequency',
                                    'TTREQUESTTIME':'TT Creation Time',
                                    'CUSTOMATTR19':'HVC_STATUS'})

#print(df_final.columns)
df_final.to_csv(pth)
parm = pth
print('csv download successfully')
excelpath = pt + '\\xlsF\\A_SEMRW.xlsm'
filepath= pth
excel_app = xlwings.App(visible=False)
excel_book = excel_app.books.open(excelpath)
# into brackets, the path of the macro
x = excel_book.macro('init')
x(filepath)
time.sleep( 3 )
conn
print('Closing Success')


