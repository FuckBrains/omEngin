#!/usr/bin/env python
# coding: utf-8

# In[11]:


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

livedb = os.getcwd() + "\\robi_live.csv"
db = os.getcwd() + "\\OMDB.csv"
semcol = os.getcwd() + "\\semcols.txt"
cat = os.getcwd() + "\\catdef.txt"
conn= MySQLdb.connect("localhost","root","admin","om2")

def hr_minus(diff):
    n = datetime.now()
    d = n - timedelta(hours=diff)
    str_d = d.strftime("%m-%d-%Y %H:%M:%S")
    return str_d

def oFn1(df, *argv, **kwargs):
    ls = []
    col = df.columns.to_list()
    for n in range(len(argv)):
        TempLs = df[argv[n]].values.tolist()
        if len(ls) == 0:
            ls = TempLs
        else:
            tls = [i + j for i, j in zip(ls, TempLs)]
            ls = tls
    ld = []
    for key,value in kwargs.items():
        if col.count(value) != 0:
            TmpLd = df[value].to_list()
            if len(ld) == 0:
                ld = TmpLd
            else:
                tld = [i + j for i, j in zip(ld, TmpLd)]
                ld = tld
        else:
            ar = np.full(df.shape[0], value)
            TmpLd = ar.tolist()
            if len(ld) == 0:
                ld = TmpLd
            else:
                tld = [i + j for i, j in zip(ld, TmpLd)]
                ld = tld
    fls = []
    for i in range(len(ld)):
        x = ls.count(ld[i])
        fls.append(x)
    colx = 'C' + str(df.shape[1])
    df[colx] = np.array(fls)
    return df

def text2list(pth):
    f = open(pth, 'r+')
    ls = []
    for i in f.readlines():
        ls.append(i.replace('\n',''))
    return ls
    
def text2dic(pth):
    f = open(pth, 'r+')
    dc = {}
    for i in f.readlines():
        a1 = i.replace('\n','')
        a2 = a1.split(':')
        dc[a2[0]] = a2[1]
    return dc
                      
def getkey(my_dict, ky):
    if ky is not None:
        for key, value in my_dict.items():
            if key in str(ky):
                return value
        else:
            return "other"

DURCAT = lambda x : '2H' if (x < 120)                 else ('4H' if (x < 240)                 else ('6H' if (x < 360)                 else ('8H' if (x < 480)                 else ('10H' if (x < 600)                 else ('12H' if (x < 720)                 else ('24H'))))))
    
TS = lambda x : '2G' if ('2G' in x)                 else ('3G' if ('3G' in x)                 else ('4G' if ('4G' in x)                 else "other"))
    
def fluc(df):
    dfdb1 = pd.read_csv(db)
    dfdb = dfdb1[['Code','Zone']]
    df0 = df.rename(columns=str.upper)
    ls = text2list(semcol)
    df1 = df0[ls]
    df1.to_csv(os.getcwd() + "\\A1.csv", index = False)
    dc = text2dic(cat)
    df1['cat'] = df1.apply(lambda x: getkey(dc, x.SUMMARY) , axis = 1)
    df1['catx'] = df1.apply(lambda x: TS(x.SUMMARY) , axis = 1)
    df1['Code'] = df1.apply(lambda x: x.CUSTOMATTR15[0:5], axis = 1)
    df = df1.merge(dfdb, on='Code')
    y = hr_minus(10)
    df['LASTOCCURRENCE'] = pd.to_datetime(df['LASTOCCURRENCE'])
    df['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].map(lambda x: x.strftime("%d/%m/%Y %H:%M:%S"))
    df = df.assign(NW = y)
    df['DUR'] = df.apply(lambda x : pd.to_datetime(x.NW) - pd.to_datetime(x.LASTOCCURRENCE) ,axis=1)
    df['DUR'] = df['DUR'].astype("i8")/1e9
    df['DUR'] = df['DUR'].apply(lambda x: x/60)
    df['DURCAT'] = df.apply(lambda x: DURCAT(x.DUR), axis = 1)
    df['LO'] = df.apply(lambda x : pd.to_datetime(x['LASTOCCURRENCE']).strftime("%y%m%d%H%M"), axis = 1)
    df['CDLO'] = df['CUSTOMATTR15'].str.cat(df['LO'])
    xdf = df[df['catx'].isin(['2G','3G','4G'])]
    xdf.to_csv(os.getcwd() + "\\A2.csv", index = False)
    xdf = xdf.replace(np.nan, 0)
    ndf = countifs(xdf,xdf['CUSTOMATTR15'],xdf['CUSTOMATTR15'],xdf['DURCAT'],xdf['DURCAT'])
    ndf = ndf.sort_values(by='cat', inplace=True, ascending=True)
    dfz = ndf.drop_duplicates(subset=['catx','CDLO'], keep='first', inplace = True)
    dfz.to_csv(os.getcwd() + "\\A3.csv", index = False)
    dfy = pd.read_csv(os.getcwd() + "\\RA11.csv")
    dfy.to_csv(os.getcwd() + "\\A4.csv", index = False)
    return ndf


def xx(ndf):
    #xdf = xdf.replace(np.nan, 0)
    #ndf = countifs(xdf,xdf['CUSTOMATTR15'],xdf['CUSTOMATTR15'],xdf['DURCAT'],xdf['DURCAT'])
    df = ndf.convert_dtypes()
    #print(df.dtypes)
    
    #print(df[['CUSTOMATTR15','cat','catx','DURCAT','CDLO']])
    print(df1)
    #df.to_csv(os.getcwd() + "\\RA7.csv", index = True)
    #return dfx5
    #print(xdff)
        
svpt = os.getcwd() + "\\OMDW.csv"
svpt2 = os.getcwd() + "\\RA10.csv"
ddf = pd.read_csv(svpt)
#print(ddf)
xy = fluc(ddf)
ddfx = pd.read_csv(svpt2)
df = xx(ddfx)
#print(df)


    
#df4['NW'] = df4.apply(lambda x: x.DURCAT + x.AB, axis = 1)
#df5 = df4[df4['NW'].isin(['<12H10','<2H2'])]
#print(df4, df4.columns, df4.shape[0])
#for i in range(len(df4)):
    #print(df4.loc[i, 'EQUIPMENTKEY'])



# In[ ]:





# In[ ]:





# In[ ]:




