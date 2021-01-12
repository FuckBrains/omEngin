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

def timedelt(diff):
    x = datetime.now ()
    d = x + timedelta (hours=diff)
    str_d = d.strftime ("%d-%m-%Y %H:%M:%S")
    return str_d

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
    df.to_csv(os.getcwd () + "\\SEMQRY.csv")
    print ('ends: ', datetime.now())
    print(df.shape[0])
    print(df.columns)
    df1 = df[df['AGENT'].isin([agent])]
    print (df.shape[0])
    print(os.getcwd () + "\\SEMQRY.csv")
    return df1

def filter_p(df,reflst,oncolumn):
    i = 0
    dfx = pd.DataFrame([])
    rw = 0
    for k in reflst:
        i = i + 1
        ndf = df[df[oncolumn].str.contains(k)]
        rw = ndf.shape[0]
        if rw >= 2:
            if i == 1:
                dfx = ndf
            else:
                dfy = pd.concat([dfx,ndf])
                dfx = dfy
                dfy = pd.DataFrame([])
    else:
        return dfx

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


DRCAT = lambda x: 'H2' if (x < 120) \
    else ('H12' if (x < 720)\
    else ('H24'))

TS = lambda x: '2G' if ('2G SITE DOWN' in x) \
    else ('2G' if ('2G CELL DOWN' in x) \
    else ('3G' if ('3G SITE DOWN' in x) \
    else ('3G' if ('3G CELL DOWN' in x) \
    else ('4G' if ('4G SITE DOWN' in x) \
    else ('4G' if ('4G CELL DOWN' in x) \
    else ('2G' if ('OML' in x) \
    else "other"))))))


def extrafeat(xdf, tmdelta = 0):
    df = xdf.assign (DURCAT='0')
    df = df.assign (LO='0')
    df = df.assign (CDLO='0')
    df = df.assign (CDLOTECH='0')
    df['DURCAT'] = df.apply (lambda x: DRCAT (x.DUR), axis=1)
    df['LO'] = df.apply (lambda x: pd.to_datetime (x['LASTOCCURRENCE'], errors='coerce', cache=True).strftime("%d%m%y%H%M"), axis=1)
    df['CDLO'] = df['CUSTOMATTR15'].str.cat (df['LO'])
    df['CDLOTECH'] = df['CDLO'].str.cat (df['CATX'])
    print('extrafeat',df.shape[0])
    return df

def filter_rmvdup_cnt(df):
    xdf = filter_p(df, ['2G', '3G', '4G'], 'CATX')
    xdf = xdf.reset_index()
    xdf = xdf.sort_values(by=['CAT','CDLO'], ascending=True)
    xdf = xdf.reset_index()
    df1 = xdf.drop_duplicates(subset=['CDLOTECH'], ignore_index=True)
    ndf = countifs(df1, xdf['CUSTOMATTR15'], xdf['CUSTOMATTR15'], xdf['DURCAT'], xdf['DURCAT'])
    odf = countifs(ndf, xdf['EQUIPMENTKEY'], xdf['EQUIPMENTKEY'], xdf['DURCAT'], xdf['DURCAT'])
    return odf
    

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
    df5 = filter_rmvdup_cnt(df4)
    

    

def sort_rvmdup(df):
    df1 = df.sort_values(by=['CAT','CDLO'], ascending=True)
    df1 = df1.drop_duplicates(subset=['CDLOTECH'], inplace=False, ignore_index=True)
    df1.to_csv (os.getcwd () + "\\FINAL13.csv", index=False)
    #df2 = df1.groupby(['DURCAT','EQUIPMENTKEY','CAT'])['CUSTOMATTR15'].count()
    pvt = df1.pivot_table(index=['CUSTOMATTR15','CAT'], columns='DURCAT', values='cnt_x', aggfunc='sum').reset_index()
    ndf = pvt[(pvt['H2'] > 2) & (pvt['H12'] > 10)]
    return ndf

def fmtmsg_techwise(ndf, name_thread_col, ls_datacol, name_catcol, cat_text):
    lss = []
    hpx = ""
    colx = ndf.columns.to_list()
    print(colx)
    df = ndf[["CUSTOMATTR15","CAT","H2","H12"]]
    for n in range(len(df)):
        cat = df.iloc[n, 1]
        if str(cat) == cat_text:
            try:
                code = df.iloc[n, 0] + ": " + str(df.iloc[n, 2]) + " | " + str(df.iloc[n, 3])
                lss.append(code)
                hpx = hpx + chr(10) + code
            except:
                pass
        else:
            pass
    print(lss)
    return hpx
        

def main(df):
    ls = ['H2', 'H12']
    df = df.astype (str)
    df1 = catmap_mod(df)
    df1 = df1.astype (str)
    df0 = prob(df1)
    df2 = sort_rvmdup(df0)
    print('2')
    df2.to_csv(os.getcwd () + "\\pvt.csv", index = False)
    df2 = pd.read_csv(os.getcwd () + "\\pvt.csv")
    df2 = df2.astype (str)
    print(df2.dtypes)
    G2 = "2G:" + chr (10) + fmtmsg_techwise (df2, 'CUSTOMATTR15', ['H2', 'H12'], 'CAT', '2') + chr (10) + chr (10)
    G2CELL = "2G CELL:" + chr (10) + fmtmsg_techwise (df2, 'CUSTOMATTR15', ['H2', 'H12'], 'CAT', '22') + chr (10) + chr (10)
    G3 = "3G:" + chr (10) + fmtmsg_techwise (df2, 'CUSTOMATTR15', ['H2', 'H12'], 'CAT', '3') + chr (10) + chr (10)
    G3CELL = "3G CELL:" + chr (10) + fmtmsg_techwise (df2, 'CUSTOMATTR15', ['H2', 'H12'], 'CAT', '33') + chr (10) + chr (10)
    G4 = "4G:" + chr (10) + fmtmsg_techwise (df2, 'CUSTOMATTR15', ['H2', 'H12'], 'CAT', '4') + chr (10) + chr (10)
    G4CELL = "4G CELL:" + chr (10) + fmtmsg_techwise (df2, 'CUSTOMATTR15', ['H2', 'H12'], 'CAT', '44') + chr (10) + chr (10)
    HD1 = "FLUCTUATION STATUS" + chr (10) + "at " + tm + chr (10) + chr (10)
    HD2 = "Code : 2Hr | H12r" + chr (10) + chr (10)
    TR1 = "Note: sites fluctuates >10 times in last 2hr and fluctuations found in last H12r"
    GG2 = "2G " + HD1 + HD2 + G2 + TR1
    GG2C = "2G CELL" + HD1 + HD2 + G2CELL + TR1
    msk = '-407548960'
    q = tmsg(msk, "SITE " + GG2)
    q = tmsg (msk, "CELL " + GG2C)
    GG3 = "3G " + HD1 + HD2 + G3 + TR1
    GG3C = "3G CELL" + HD1 + HD2 + G3CELL + TR1
    q = tmsg (msk, "SITE " + GG3)
    q = tmsg (msk, "CELL " + GG3C)
    GG4 = "4G " + HD1 + HD2 + G4 + TR1
    GG4C = "4G CELL" + HD1 + HD2 + G4CELL + TR1
    q = tmsg (msk, "SITE " + GG4)
    q = tmsg (msk, "CELL " + GG4C)
    print('done')

def filter_rmvdup_cnt(df):
    print(df.columns)
    #xdf = df.reset_index()
    #xdf = filter_p(df, ['2G', '3G', '4G'], 'CATX')
    #xdf = xdf.reset_index()
    df2 = df[~df['CATX'].isin(['other'])]
    df = df2.reset_index()
    xdf = df.sort_values(by=['CAT','CDLO'], ascending=True)
    xdf = xdf.reset_index()
    df1 = xdf.drop_duplicates(subset=['CDLOTECH'], ignore_index=True)
    ndf = countifs(df1, xdf['CUSTOMATTR15'], xdf['CUSTOMATTR15'], xdf['DURCAT'], xdf['DURCAT'])
    odf = countifs(ndf, xdf['EQUIPMENTKEY'], xdf['EQUIPMENTKEY'], xdf['DURCAT'], xdf['DURCAT'])
    print(odf)
    return odf

def catmap_md(df):
    print("strart operation..............")
    dfdb1 = pd.read_csv (db)
    dfdb = dfdb1[['Code', 'Zone']]
    df0 = df.rename (columns=str.upper)
    df1 = df0
    #ls = text2list (semcol)
    #df1 = df0[ls]
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
    df5 = filter_rmvdup_cnt(df4)
    return df5


print(os.getcwd())
svpt = os.getcwd () + "\\OMTX.csv"
df = pd.read_csv (svpt, low_memory=False)
catmap_md(df)
