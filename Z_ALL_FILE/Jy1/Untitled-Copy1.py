#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import cx_Oracle
import os
from datetime import *
from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *

nw = datetime.now()
dtst = nw.strftime ("%d%m%Y%H%M%S")
fl = os.getcwd() + "\\dw\\" + dtst + ".csv"
#print(fl)

def sem_view_filter_cols():
    df = pd.read_csv(os.getcwd() + "\\col_filter_semdb_view_non_macro.csv")
    ls = df.iloc[:,0].to_list()
    x = ",".join(list(ls))
    return x

def timedelt(diff):
    x = datetime.now ()
    d = x + timedelta (hours=diff)
    str_d = d.strftime ("%d-%m-%Y %H:%M:%S")
    return str_d

def tmx(t1=False):
    nw = datetime.now()
    dtst = nw.strftime("%d-%m-%Y %H:%M:%S")
    if t1 == False:
        print("Stat Time: ", dtst)
        return nw
    else:
        x = (parse("22-12-2020 01:05") - datetime.now()).seconds / 60
        print("End Time: ", dtst)
        print("Time Consumed: ", x, " mins")
        
conn = cx_Oracle.connect ('SOC_READ','soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
print (conn.version)
    
def qryex(qr = False, flname = fl):
    q = ""
    if qr == False:
        q1 = "select " + sem_view_filter_cols() + " FROM SEMHEDB.ALERTS_STATUS_V_FULL  Where SEVERITY>0"
    else:
        q1 = "select " + "*" + " FROM SEMHEDB.ALERTS_STATUS_V_FULL WHERE " + str(qr)
    print(q1)
    st = tmx()
    df = pd.read_sql(q1, con = conn)
    et = tmx(st)
    df.to_csv(os.getcwd() + "\\dw\\" + flname)
    return df
    
def timebetween(t1,t2):
    d1 = parse(t1)
    d2 = parse(t2)
    print(type(d1))
    dd = "LASTOCCURRENCE BETWEEN TO_DATE('" + d1.strftime("%d-%m-%Y %H:%M:%S") + "','DD-MM-YYYY HH24:MI:SS') AND TO_DATE('" +  d2.strftime("%d-%m-%Y %H:%M:%S") + "','DD-MM-YYYY HH24:MI:SS')"
    return dd

#######################################################################################
def qr1():
    x21 = timebetween('24-12-2020 12:08','24-12-2020 12:18')
    Y21= qryex(x21,'EFDSDFSDFS.csv')

def qr2():
    x21 = timebetween('26-12-2020 00:08','26-12-2020 23:50')
    x22 = " CUSTOMATTR3 LIKE 'PHYSICAL PORT DOWN' and " + x21 
    df= qryex(x22,'all_oneday_ip.csv')

qr2()

#xx = (parse("22-12-2020 01:05") - datetime.now()).seconds / 60
#print(xx)
#x = relativedelta(
    #print(datetime.strptime("22-12-2020 01:05","%d-%m-%Y %H:%M:%S"))- datetime.strptime(datetime.now(),"%d-%m-%Y %H:%M:%S").seconds / 60
#print(x)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




