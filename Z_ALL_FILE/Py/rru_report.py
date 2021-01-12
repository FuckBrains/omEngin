import pandas as pd
import numpy as np
import os
from datetime import date
import win32com.client
import subprocess
import time

def rru_lastday(tbl,usr, pas, selcol):
    conn = cx_Oracle.connect(usr, pas, 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
    print(conn.version)
    tim = time.localtime()
    tdy = date.today()
    foldr = os.getcwd() + "\\download\\" + tdy.strftime('%m%d%y') + time.strftime("%H%M", tim) + '_' + tbl + '.csv'
    dy_p = odt.day_minus(1)
    dy_f = odt.day_plus(1)
    Q1 = "FROM " + tbl + " WHERE TYPE=1 AND SUMMARY LIKE 'ERI-RRU THEFT' "
    Q2 = "AND (LASTOCCURRENCE BETWEEN TO_DATE('" + dy_p + "','DD-MM-RRRR') AND TO_DATE('" + dy_f + "','DD-MM-RRRR'))"
    QF = "SELECT" + selcol + Q1 + Q2
    print(QF)
    print('----------------')
    df = pd.read_sql(QF, con=conn)
    conn.close()
    df.to_csv(os.getcwd() + "\\" + "DW1709.csv")
    return df.to_dict()

td = date.today()
pt = os.getcwd() + "\\rru_download\\" + "RRU_" + td.strftime('%Y-%m-%d') + ".csv"
ptmacro = os.getcwd() + "\\rru_download\\rru_mac.xlsm"
xlcls = os.getcwd() + "\\rru_download\\xlcls.vbs"

cols = ["SERIAL","EQUIPMENTKEY","CUSTOMATTR15","SUMMARY","LASTOCCURRENCE","CLEARTIMESTAMP","ALARMDETAILS"]
single = os.getcwd() + "\\" + "DWRRU.csv"
dcc1 = rru_lastday('SEMHEDB.ALERTS_STATUS','SOC_READ','soc_read',' * ')
df = pd.DataFrame(dcc1)
df2 = df[cols]
code= [df2['CUSTOMATTR15'].value_counts(dropna=False)]
ndf = pd.DataFrame(code).T
ndf.to_csv(pt)

print(pt)
xl = win32com.client.Dispatch("Excel.Application")
xl.Visible = True
book = xl.Workbooks.Open(ptmacro)
time.sleep( 50 )
xl.Application.Run("rru_mac.xlsm!init_rru", pt) #With Parameter
time.sleep( 10 )
xl.Application.Quit()
print('all sucess with python')
time.sleep( 10 )