import pandas as pd
import cx_Oracle
import time
import os
from datetime import date
from datetime import timedelta
from datetime import datetime
pt = os.getcwd()
today = date.today()
omdb = os.getcwd() + "\\" + "OMDB.csv"
ExTime = int(time.strftime("%M"))
pntxt = pt + '\\' + 'pntxt.txt'
savedirr = pt + '\\' + 'OMTX.csv'


print(ExTime)

def timex():
    t = time.localtime()
    curr_tm = time.strftime("%H%M", t)
    return curr_tm

def day_diff(diff):
    d = datetime.now() + timedelta(days=diff)
    str_d = d.strftime("%d-%b-%Y")
    return str_d

def qry_all_active(tbl,usr, pas, selcol,Q3=False):
    conn = cx_Oracle.connect(usr, pas, 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
    print(conn.version)
    tim = time.localtime()
    foldr = os.getcwd() + "\\download\\" + today.strftime('%m%d%y') + time.strftime("%H%M", tim) + '_' + tbl + '.csv'
    dy_p = day_diff(-7)
    dy_f = day_diff(1)
    Q1 = "FROM " + tbl + " WHERE TYPE=1 AND Severity BETWEEN 1 AND 5 "
    Q2 = "AND (LASTOCCURRENCE BETWEEN TO_DATE('" + dy_p + "','DD-MM-RRRR') AND TO_DATE('" + dy_f + "','DD-MM-RRRR'))"
    QF = "SELECT" + selcol + Q1 + Q2
    print(QF)
    print('----------------')
    print(timex())
    df = pd.read_sql(QF, con=conn)
    print(timex())
    df1 = df[['SERIAL','EQUIPMENTKEY','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','CUSTOMATTR3']]
    df1.to_csv(savedirr)
    print(savedirr)
    return df1

def qry_all_last_day(tbl,usr, pas, selcol,Q3):
    conn = cx_Oracle.connect(usr, pas, 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
    print(conn.version)
    tim = time.localtime()
    foldr = os.getcwd() + "\\download\\" + today.strftime('%m%d%y') + time.strftime("%H%M", tim) + '_' + tbl + '.csv'
    d1 = datetime.now() + timedelta(days=-1)
    dy_p = d1.strftime("%d-%b-%Y")
    d2 = datetime.now() + timedelta(days=1)
    dy_f = d2.strftime("%d-%b-%Y")
    Q1 = "FROM " + tbl + " WHERE TYPE=1 " #AND Severity BETWEEN 1 AND 5 "
    Q2 = "AND (LASTOCCURRENCE BETWEEN TO_DATE('" + dy_p + "','DD-MM-RRRR') AND TO_DATE('" + dy_f + "','DD-MM-RRRR'))"
    QF = "SELECT" + selcol + Q1 + Q2 + Q3
    print(QF)
    print('----------------')
    t = time.localtime()
    curr_tm = time.strftime("%H%M", t)
    print('start at ', curr_tm)
    df = pd.read_sql(QF, con=conn)
    t = time.localtime()
    curr_tm = time.strftime("%H%M", t)
    print('start at ', curr_tm)
    df1 = df[['SERIAL','NODE','EQUIPMENTKEY','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','CUSTOMATTR3','EventId','X733CorrNotif','X733EventType','X733ProbableCause','X733SpecificProb','CorrelateTopologyKey','TTSequence','TTStatus','TTUpdate','TTUser','CustomAttr10','CustomAttr11','CustomAttr12','CustomAttr13','CustomAttr5','CustomAttr26']]
    df1.to_csv(savedirr)
    print(savedirr)
    return df1

def qry_all():
    df = qry_all_last_day('SEMHEDB.ALERTS_STATUS','SOC_READ','soc_read',' * '," AND AGENT IN ('U2000 TX','Ericsson OSS','EricssonOSS','Huawei U2000 vEPC','Huawei U2020','LTE_BR1_5','MV36-PFM3-MIB','BusinessRule14','BusinessRule14_ERI_ABIP')")
    return df
