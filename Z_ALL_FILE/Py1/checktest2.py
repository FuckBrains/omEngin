import pandas as pd
import cx_Oracle
import time
import os
from datetime import date
from datetime import datetime
from datetime import timedelta
import win32com.client
from dateutil.relativedelta import *

today = date.today()
td1 = today.strftime('%Y%m')
lastmnth = datetime.now() - relativedelta(months=1)
td2 = lastmnth.strftime('%Y%m')

def filename():
    pt = os.getcwd() + "\\" + "csv_download\\"
    t = time.localtime()
    folderName1 = today.strftime('%m%d%y')
    folderName2 = time.strftime("%H%M", t)
    pth = os.path.join(pt + folderName1 + folderName2 + '.csv')
    return pth

def qry_delta(from_min,to_min): 
    tday = date.today()
    tmdlta_from_now = datetime.now() - timedelta('minutes='+ int(from_min))
    tmdlta_to_now = datetime.now() - timedelta('minutes='+ int(to_min))
    qry_from = tmdlta_from_now.strftime('%Y-%m-%d %H:%M:%S')
    qry_to = tmdlta_to_now.strftime('%Y-%m-%d %H:%M:%S')
    dyn_date = "TO_DATE('" + qry_from + "','dd/mm/yyyy hh:mi:ss') AND TO_DATE('" + qry_to + "','dd/mm/yyyy hh:mi:ss')"
    return dyn_date


col = '*'
smry = "'2G SITE DOWN'"
query_p1 = 'SELECT ' + col + ' FROM ALERTS_STATUS PARTITION (STATUS_MDA_SEM_DAT_' + td1 + ') WHERE '
query_p2 = "(TO_DATE(CLEARTIMESTAMP,'DD-MM-RRRR')='01-JAN-1970') AND (SUMMARY=" + smry + ')'
query_p3 = 'IN (SELECT ' + col + 'FROM ALERTS_STATUS PARTITION (STATUS_MDA_SEM_DAT_' + td2 + ') WHERE '
query_p4 = 'SUMMARY=' + smry + " AND (TO_DATE(CLEARTIMESTAMP,'DD-MM-RRRR')='01-JAN-1970')) AND CUSTOMATTR15!='UNKNOWN')"

qry_type1 = query_p1 + query_p2
print(qry_type1)



