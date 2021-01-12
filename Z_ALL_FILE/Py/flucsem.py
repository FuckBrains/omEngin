import os, cx_Oracle
import time as ti
import requests
import numpy as np
import pandas as pd
from fn import *
from oDT import *
from datetime import *

livedb = os.getcwd () + "\\robi_live.csv"
db = os.getcwd () + "\\OMDB.csv"
semcol = os.getcwd () + "\\semcols.txt"
CAT = os.getcwd () + "\\CATdef.txt"

nw = datetime.now()
td = nw.strftime("%Y_%b_%d")
mylog = os.getcwd() + "\\log"
todaylog = os.getcwd() + "\\log\\" + td
print(todaylog)

try:
    os.makedirs(mylog)
    os.makedirs(todaylog)
    print("folder created successfully")
except:
    try:
        os.makedirs(todaylog)
    except:
        print(" todayslog folder exits + ")

n = datetime.now ()
td = n.today()
#print(str(td) + "00:00:00")
tm = n.strftime("%H:%M") + " on " + n.strftime ("%m-%d-%Y")


def w2t(text):
    nx = datetime.now ()
    file1 = os.getcwd() + "\\" + nx.strftime("%m%d%H%M%S") + ".txt"
    file2 = os.getcwd() + "\\dump\\" + nx.strftime("%m%d%H%M%S") + ".txt"
    try:
        try:
            f = open(file2, 'a+')
        except:
            f = open(file1, 'a+')
        f.write("\n")
        f.write(text)
        f.close()
    except:
        pass
    print(file)
    return ""

def tmsg(chatid,msg):
    TOK = "1176189570:AAEfPi9TIZIbnhWi4Ko6KQev2Iv7UbMw5js"
    url = "https://api.telegram.org/bot" + TOK + "/sendMessage?chat_id=" + str(chatid) + "&text=" + msg
    requests.get(url)
    return ""

def hr_minus(diff):
    x = datetime.now ()
    d = x - timedelta (hours=diff)
    str_d = d.strftime ("%m-%d-%Y %H:%M:%S")
    return str_d

def lasthr(diff = 1):
    x = datetime.now ()
    d = x - timedelta (hours=diff)
    str_d = d.strftime ("%H")
    return str_d

def timedelt(diff):
    x = datetime.now ()
    d = x + timedelta (hours=diff)
    str_d = d.strftime ("%d-%m-%Y %H:%M:%S")
    return str_d

def semqry():
    conn = cx_Oracle.connect ('SOC_READ','soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
    print (conn.version)
    agent = ['U2000 TX','Ericsson OSS','EricssonOSS','Huawei U2000 vEPC','Huawei U2020','LTE_BR1_5','MV36-PFM3-MIB','BusinessRule14','BusinessRule14_ERI_ABIP']
    cols = "SERIAL,NODE,AGENT,ALERTGROUP,X733EVENTTYPE,X733SPECIFICPROB,CLASS,GEOINFO,CUSTOMATTR3,CUSTOMATTR5,CUSTOMATTR26,TTSEQUENCE,ALARMDETAILS,EQUIPMENTKEY,SITECODE,SUMMARY,LASTOCCURRENCE,CLEARTIMESTAMP"
    #cols = "SERIAL,NODE,AGENT,SUMMARY,LASTOCCURRENCE,CLEARTIMESTAMP,EQUIPMENTKEY,SITECODE"
    q1 = "SELECT " +  cols + " FROM SEMHEDB.ALERTS_STATUS_V_FULL WHERE "
    x = n
    hr = x.strftime('%H')
    STDT = timedelt(-int(hr))
    ENDT = timedelt(1)
    q2 = "LASTOCCURRENCE BETWEEN TO_DATE('" + STDT + "','DD-MM-YYYY HH24:MI:SS') AND TO_DATE('" + ENDT + "','DD-MM-YYYY HH24:MI:SS')"
    q3 = q1 + q2
    print(q3)
    print('starts: ', datetime.now())
    df = pd.read_sql(q3, con=conn)
    print ('ends: ', datetime.now())
    print(df.shape[0])
    print(df.columns)
    try:
        df = df.rename(columns = {'SITECODE':'CUSTOMATTR15'})
    except:
        pass
    #df1 = df[df['AGENT'].isin([agent])]
    #print (df.shape[0])
    lscol = ['SERIAL','NODE','EQUIPMENTKEY','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP']
    ddf = df[lscol]
    ddf.to_csv(os.getcwd () + "\\SEMQRY.csv", index=False)
    ti.sleep(2)
    return ddf

def semqry_dummy():
    df = pd.read_csv(os.getcwd () + "\\SEMQRY.csv")
    return df