import os, cx_Oracle
from datetime import *
import requests
import MySQLdb
import numpy as np
import pandas as pd
from fn import *
from oDT import *

livedb = os.getcwd () + "\\robi_live.csv"
db = os.getcwd () + "\\OMDB.csv"
semcol = os.getcwd () + "\\semcols.txt"
CAT = os.getcwd () + "\\CATdef.txt"
try:
    mysqlconn = MySQLdb.connect ("localhost", "root", "admin", "om2")
except:
    mysqlconn = ""


n = datetime.now ()
tm = n.strftime("%H:%M") + " on " + n.strftime ("%m-%d-%Y")

def hr_minus(diff):
    x = datetime.now ()
    d = x - timedelta (hours=diff)
    str_d = d.strftime ("%m-%d-%Y %H:%M:%S")
    return str_d

def timedelt(diff):
    x = datetime.now ()
    d = x + timedelta (hours=diff)
    str_d = d.strftime ("%d-%m-%Y %H:%M:%S")
    return str_d


def text2list(pth):
    f = open (pth, 'r+')
    ls = []
    for i in f.readlines ():
        ls.append (i.replace ('\n', ''))
    return ls


def text2dic(pth):
    f = open (pth, 'r+')
    dc = {}
    for i in f.readlines():
        a1 = i.replace ('\n', '')
        a2 = a1.split (':')
        dc[a2[0]] = a2[1]
    return dc


def getkey(my_dict, ky):
    if ky is not None:
        for key, value in my_dict.items ():
            if key in str (ky):
                return value
        else:
            return "other"


DRCAT = lambda x: '2H' if (x < 120) \
    else ('4H' if (x < 240)\
    else ('6H' if (x < 360)\
    else ('12H' if (x < 720)\
    else ('24H' if (x < 1440)\
    else ('48H' if (x < 2880)\
    else ('72H'))))))

TS = lambda x: '2G' if ('2G' in x) \
    else ('3G' if ('3G' in x) \
    else ('4G' if ('4G' in x) \
    else ('OML' if ('2G' in x) \
    else "other")))


def extrafeat(xdf, tmdelta = 0):
    xdf = xdf.rename (columns=str.upper)
    df = xdf.assign (DURCAT='0')
    df = df.assign (LO='0')
    df = df.assign (CDLO='0')
    df = df.assign (CDLOTECH='0')
    df['DURCAT'] = df.apply (lambda x: DRCAT (x.DUR), axis=1)
    df['LO'] = df.apply (lambda x: pd.to_datetime (x['LASTOCCURRENCE'], errors='coerce', cache=True).strftime("%d%m%y%H%M"), axis=1)
    df['CDLO'] = df['CUSTOMATTR15'].str.cat (df['LO'])
    df['CDLOTECH'] = df['CDLO'].str.cat (df['CATX'])
    print('done duration')
    return df

def catmap_mod(df):
    print("strart operation..............")
    dfdb1 = pd.read_csv (db)
    dfdb = dfdb1[['Code', 'Zone']]
    df0 = df.rename (columns=str.upper)
    ls = text2list (semcol)
    df1 = df0[ls]
    dc = text2dic (CAT)
    df1 = df1.assign (CAT='0')
    df1 = df1.assign (CATX='0')
    df1 = df1.assign (Code='0')
    df1['CAT'] = df1.apply (lambda x: getkey (dc, x.SUMMARY), axis=1)
    df1['CATX'] = df1.apply (lambda x: TS (x.SUMMARY), axis=1)
    df1['Code'] = df1.apply (lambda x: x.CUSTOMATTR15[0:5], axis=1)
    df2 = df1.merge (dfdb, on='Code')
    try:
        df3 = DateDiff(df2, "DUR", "LASTOCCURRENCE")
    except:
        df3 = datediff_ondf(df2, "DUR", 'LASTOCCURRENCE')
    df4 = extrafeat(df3)
    xdf = df4.replace (np.nan, 0)
    ndf = countifs(xdf, xdf['CUSTOMATTR15'], xdf['CUSTOMATTR15'], xdf['DURCAT'], xdf['DURCAT'])
    odf = countifs(ndf, xdf['EQUIPMENTKEY'], xdf['EQUIPMENTKEY'], xdf['DURCAT'], xdf['DURCAT'])
    odf.to_csv (os.getcwd () + "\\FINAL12.csv", index=False)
    return odf

def sort_rvmdup(df):
    df1 = df[~df['CATX'].isin(['other']) & ~df['CAT'].isin(['other'])]
    df1 = df1.sort_values(by=['CAT','CDLO'], ascending=True)
    df1 = df1.drop_duplicates(subset=['CDLOTECH'], inplace=False, ignore_index=True)
    #df2 = df1.groupby(['DURCAT','EQUIPMENTKEY','CAT'])['CUSTOMATTR15'].count()
    pvt = df1.pivot_table(index=['CUSTOMATTR15','CAT'], columns='DURCAT', values='cnt_x', aggfunc='sum').reset_index()
    ndf = pvt[(pvt['72H'] > 10) & (pvt['48H'] > 2)]
    return ndf

def fmtmsg_techwise(df, name_thread_col, ls_datacol, name_catcol, cat_text):
    lss = []
    heap = ''
    hp = ""
    hpx = ""
    for n in range(len(df)):
        code = df.loc[n, name_thread_col]
        cat = df.loc[n, name_catcol]
        if str(cat) == str(cat_text):
            for i in range(len(ls_datacol)):
                if hp == "":
                    hp = df.loc[n, ls_datacol[i]]
                else:
                    hp = hp + " | " + df.loc[n, ls_datacol[i]]
            hpx = code + ": " + hp
            if heap == "":
                heap = hpx
                hp = ""
            else:
                heap = heap + chr(10) + hpx
                hp = ""
    return heap

def tmsg(chatid,msg):
    TOK = "1176189570:AAEfPi9TIZIbnhWi4Ko6KQev2Iv7UbMw5js"
    url = "https://api.telegram.org/bot" + TOK + "/sendMessage?chat_id=" + str(chatid) + "&text=" + msg
    requests.get(url)
    return ""

def semqry():
    conn = cx_Oracle.connect ('SOC_READ','soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
    print (conn.version)
    agent = ['U2000 TX','Ericsson OSS','EricssonOSS','Huawei U2000 vEPC','Huawei U2020','LTE_BR1_5','MV36-PFM3-MIB','BusinessRule14','BusinessRule14_ERI_ABIP']
    cols = "SERIAL,NODE,AGENT,ALERTGROUP,SEVERITY,LOCALSECOBJ,X733EVENTTYPE,X733SPECIFICPROB,MANAGEDOBJCLASS,GEOINFO,CUSTOMATTR3,CUSTOMATTR5,CUSTOMATTR25,TTSEQUENCE,TTSTATUS,SRCDOMAIN,CUSTOMATTR26,OUTAGEDURATION,TALLY,ALARMDETAILS,EQUIPMENTKEY,CUSTOMATTR15,SUMMARY,LASTOCCURRENCE,CLEARTIMESTAMP"
    q1 = "SELECT " +  cols + " FROM SEMHEDB.ALERTS_STATUS WHERE "
    STDT = timedelt(-22)
    ENDT = timedelt(1)
    q2 = "LASTOCCURRENCE BETWEEN TO_DATE('" + STDT + "','DD-MM-YYYY HH24:MI:SS') AND TO_DATE('" + ENDT + "','DD-MM-YYYY HH24:MI:SS')"
    q3 = q1 + q2
    print(q3)
    print('starts: ', datetime.now())
    df = pd.read_sql(q3, con=conn)
    print ('ends: ', datetime.now())
    print(df.shape[0])
    print(df.columns)
    df1 = df[df['AGENT'].isin([agent])]
    print (df.shape[0])
    df1.to_csv(os.getcwd () + "\\SEMQRY.csv")
    return df1

def main(df):
    ls = ['2H', '12H']
    df1 = catmap_mod(df)
    df2 = sort_rvmdup(df1)
    print('2')
    df2.to_csv(os.getcwd () + "\\pvt.csv")
    df2 = df2.astype (str)
    G2 = "2G:" + chr (10) + fmtmsg_techwise (df2, 'CUSTOMATTR15', ls, 'CAT', '2') + chr (10) + chr (10)
    G3 = "3G:" + chr (10) + fmtmsg_techwise (df2, 'CUSTOMATTR15', ls, 'CAT', '3') + chr (10) + chr (10)
    G4 = "3G:" + chr (10) + fmtmsg_techwise (df2, 'CUSTOMATTR15', ls, 'CAT', '4') + chr (10) + chr (10)
    HD1 = "SITE FLUCTUATION COUNT" + chr (10) + "at " + tm + chr (10) + chr (10)
    HD2 = "Code : 2Hr | 12Hr" + chr (10) + chr (10)
    TR1 = "Note: sites fluctuates 2+ in last 2hr and 10+ in last 12hr"
    GG2 = "2G " + HD1 + HD2 + G2 + TR1
    msk = '-407548960'
    q = tmsg(msk, GG2)
    GG3 = "3G " + HD1 + HD2 + G3 + TR1
    q = tmsg (msk, GG3)
    GG4 = "4G " + HD1 + HD2 + G4 + TR1
    q = tmsg (msk, GG4)
    print('done')

#svpt = os.getcwd () + "\\OMTX.csv"
df = semqry()
xxx = main(df)
#svpt = os.getcwd () + "\\pvt.csv"
#df = pd.read_csv (svpt, low_memory=False)

