#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import time
from datetime import date
import omdtfn as odt
import pandas as pd
import cx_Oracle

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

def all_active(tbl,usr, pas, selcol):
    conn = cx_Oracle.connect(usr, pas, 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
    print(conn.version)
    tim = time.localtime()
    tdy = date.today()
    foldr = os.getcwd() + "\\download\\" + tdy.strftime('%m%d%y') + time.strftime("%H%M", tim) + '_' + tbl + '.csv'
    dy_p = odt.day_minus(7)
    dy_f = odt.day_plus(1)
    Q1 = "FROM " + tbl + " WHERE TYPE=1 AND Severity BETWEEN 1 AND 5 "
    Q2 = "AND (LASTOCCURRENCE BETWEEN TO_DATE('" + dy_p + "','DD-MM-RRRR') AND TO_DATE('" + dy_f + "','DD-MM-RRRR'))"
    QF = "SELECT" + selcol + Q1 + Q2
    print(QF)
    print('----------------')
    df = pd.read_sql(QF, con=conn)
    conn.close()
    df.to_csv(os.getcwd() + "\\" + "active_all.csv")
    return df.to_dict()

