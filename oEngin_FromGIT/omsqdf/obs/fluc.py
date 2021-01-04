import sys, os
import pandas as pd
import MySQLdb
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import *
from dateutil.relativedelta import *
import numpy as np
from fn import *
from oDT import *
import fnfn as fx

livedb = os.getcwd () + "\\robi_live.csv"
db = os.getcwd () + "\\OMDB.csv"
semcol = os.getcwd () + "\\semcols.txt"
CAT = os.getcwd () + "\\CATdef.txt"
conn = MySQLdb.connect ("localhost", "root", "admin", "om2")


def hr_minus(diff):
    n = datetime.now ()
    d = n - timedelta (hours=diff)
    str_d = d.strftime ("%m-%d-%Y %H:%M:%S")
    return str_d


def oFn1(df, *argv, **kwargs):
    ls = []
    col = df.columns.to_list ()
    for n in range (len (argv)):
        TempLs = df[argv[n]].values.tolist ()
        if len (ls) == 0:
            ls = TempLs
        else:
            tls = [i + j for i, j in zip (ls, TempLs)]
            ls = tls
    ld = []
    for key, value in kwargs.items ():
        if col.count (value) != 0:
            TmpLd = df[value].to_list ()
            if len (ld) == 0:
                ld = TmpLd
            else:
                tld = [i + j for i, j in zip (ld, TmpLd)]
                ld = tld
        else:
            ar = np.full (df.shape[0], value)
            TmpLd = ar.tolist ()
            if len (ld) == 0:
                ld = TmpLd
            else:
                tld = [i + j for i, j in zip (ld, TmpLd)]
                ld = tld
    fls = []
    for i in range (len (ld)):
        x = ls.count (ld[i])
        fls.append (x)
    colx = 'C' + str (df.shape[1])
    df[colx] = np.array (fls)
    return df


def text2list(pth):
    f = open (pth, 'r+')
    ls = []
    for i in f.readlines ():
        ls.append (i.replace ('\n', ''))
    return ls


def text2dic(pth):
    f = open (pth, 'r+')
    dc = {}
    for i in f.readlines ():
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

def duration(xdf, tmdelta = 0):
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
    ndf = countifs (xdf, xdf['CUSTOMATTR15'], xdf['CUSTOMATTR15'], xdf['DURCAT'], xdf['DURCAT'])
    odf = countifs (ndf, xdf['EQUIPMENTKEY'], xdf['EQUIPMENTKEY'], xdf['DURCAT'], xdf['DURCAT'])
    odf.to_csv (os.getcwd () + "\\FINAL12.csv", index=False)
    

def catmap(df):
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
        xdf = df2[df2['CAT'].isin (['2', '3', '4','22', '33', '44'])]
    except:
        try:
            xdf = df2[df2['CATX'].isin (['2', '3', '4','22', '33', '44'])]
        except:
            xdf = df2[df2['CATX'].isin (['2G', '3G', '4G'])]
    xdf.to_csv(os.getcwd () + "\\C7.csv")
    xdf = xdf.convert_dtypes()
    return xdf

def fluc(df):
    dfx = catmap(df)
    print('1')
    try:
        xdy = DateDiff(dfx, "DUR", "LASTOCCURRENCE")
    except:
        xdy = datediff_ondf(dfx, "DUR", 'LASTOCCURRENCE')
    xdf = duration(xdy)
    xdf.to_csv (os.getcwd () + "\\A2.csv", index=False)
    xdf = xdf.replace (np.nan, 0)
    ndf = countifs (xdf, xdf['CUSTOMATTR15'], xdf['CUSTOMATTR15'], xdf['DURCAT'], xdf['DURCAT'])
    ndf.to_csv (os.getcwd () + "\\FINAL0.csv", index=False)
    ndf = ndf.sort_values (by='CAT', inplace=True, ascending=True)
    dfz = ndf.drop_duplicates (subset=['CATX', 'CDLO'], keep='first', inplace=True)
    dfz.to_csv (os.getcwd () + "\\A3.csv", index=False)
    dfy = pd.read_csv (os.getcwd () + "\\A3.csv")
    dfy.to_csv (os.getcwd () + "\\FINAL1.csv", index=False)
    return ndf



svpt = os.getcwd () + "\\OMTX.csv"
df = pd.read_csv (svpt, low_memory=False)
df1 = catmap_mod(df)
print('done')
#print(xdf)
#PP(xdf)
#print(df1['LASTOCCURRENCE'])
#print('df1 get')
#df2 = duration(df1)
#xy = duration(ddf)
#ddfx = pd.read_csv (svpt2)
#df = xx (ddfx)
# print(df)
#df = ndf.convert_dtypes ()

# df4['NW'] = df4.apply(lambda x: x.DURCAT + x.AB, axis = 1)
# df5 = df4[df4['NW'].isin(['<12H10','<2H2'])]
# print(df4, df4.columns, df4.shape[0])
# for i in range(len(df4)):
# print(df4.loc[i, 'EQUIPMENTKEY'])
