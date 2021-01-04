import pandas as pd
import cx_Oracle
import os
from datetime import *
from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *

def sem_view_filter_cols():
    df = pd.read_csv(os.getcwd() + "\\DevMeterials\\col_filter_semdb_view_non_macro.csv")
    ls = df.iloc[:,0].to_list()
    x = ",".join(list(ls))
    return x

def timedelt(diff):
    x = datetime.now ()
    d = x + timedelta (minutes=diff)
    str_d = d.strftime ("%d-%m-%Y %H:%M:%S")
    return str_d


def parse_date_fuzzy(string, first='day'):
    try:
        if first == 'day':
            x = parse(string, fuzzy=True, dayfirst=True)
        elif first == 'year':
            x = parse(string, fuzzy=True, yearfirst=True)
        else:
            x = parse(string, fuzzy=True)
        return x.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return ""

    
def timebetween(t1,t2):
    d1 = parse_date_fuzzy(t1)
    d2 = parse_date_fuzzy(t2)
    dd = "LASTOCCURRENCE BETWEEN TO_DATE('" + d1 + "','DD-MM-YYYY HH24:MI:SS') AND TO_DATE('" + d2 + "','DD-MM-YYYY HH24:MI:SS')"
    print(dd)

nw = datetime.now()
dtst = nw.strftime ("%d%m%Y%H%M%S")
flname = os.getcwd() + "\\" + dtst + "csv"

def qry(qq, savefile=dtst):
    conn = cx_Oracle.connect ('SOC_READ','soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
    print (conn.version)
    q1 = "select " + sem_view_filter_cols() + " FROM SEMHEDB.ALERTS_STATUS_V_FULL WHERE "
    qry = q1 + qq
    print(qry)
    #df = pd.read_sql(qry, con = conn)
    #print(df)
    #df.to_csv(os.getcwd() + "\\dw.csv", index = False)
    #print("success")
    
qry("SEVERITY>0")
    
    
def customize():
    print (conn.version)
    allactive = "select " + sem_view_filter_cols() + " FROM SEMHEDB.ALERTS_STATUS_V_FULL  Where SEVERITY>0"
    print(allactive)
    df = pd.read_sql(allactive, con = conn)
    print(df)
    df.to_csv(os.getcwd() + "\\dw.csv", index = False)
    
#timebetween("22-12-2020 01:00","22-12-2020 02:00")