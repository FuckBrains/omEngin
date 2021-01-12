#!/usr/bin/env python
# coding: utf-8

# In[4]:


import sys, os
import pandas as pd
import MySQLdb
from fnfn import *

livedb = os.getcwd() + "\\robi_live.csv"
db = os.getcwd() + "\\OMDB.csv"
semcol = os.getcwd() + "\\semcols.txt"
cat = os.getcwd() + "\\catdef.txt"
conn= MySQLdb.connect("localhost","root","admin","om2")

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

DURCAT = lambda x : '<2H' if (x < 120)                 else ('<12H' if (x < 240)                 else ('<24H' if (x < 360)                 ))
    

def part1():
    dfdb = pd.read_csv(db)
    df = pd.read_sql('select * from big5', con = conn)
    df0 = df.rename(columns=str.upper)
    ls = text2list(semcol)
    df1 = df0[ls]
    dc = text2dic(cat)
    df1['cat'] = df1.apply(lambda x: getkey(dc, x.SUMMARY) , axis = 1)
    df1['Code'] = df1.apply(lambda x: x.CUSTOMATTR15[0:5], axis = 1)
    df2 = df1.merge(dfdb, on='Code')
    df2['LASTOCCURRENCE'] = pd.to_datetime(df2['LASTOCCURRENCE'], errors='coerce')
    #df2['CLEARTIMESTAMP'] = pd.to_datetime(df2['CLEARTIMESTAMP'], errors='coerce')
    df2['DUR'] = df2.apply(lambda x : abs(datetime.now() - x['LASTOCCURRENCE']) ,axis=1)
    df2['DUR'] = df2['DUR'].astype('timedelta64[m]')
    df2['DURCAT'] = df2.apply(lambda x: DURCAT(x.DUR), axis = 1)
    xdf = df2[df2['cat'].isin(['2G','3G','4G']) & df2['DURCAT'].isin(['<2H','<12H'])]
    dfx1 = oFn1(xdf,['EQUIPMENTKEY','cat','DURCAT'], EQUIPMENTKEY='EQUIPMENTKEY', cat ='cat', DURCAT = '<2H')
    print(dfx1)
    dfx2 = oFn1(dfx1,['EQUIPMENTKEY','cat','DURCAT'], EQUIPMENTKEY='EQUIPMENTKEY', cat ='cat', DURCAT = '<12H')
    return dfx2


class sem:
    def __init__(self, ndf):
        self.mdf = ndf
        self.df = ndf
    def P1(self, df):
        print('x')
        
#svpt = os.getcwd() + "\\OMT.csv"
df = part1()
print(df)

def xx():
    xdf = df[df['cat'].isin(['2G','3G','4G']) & df['DURCAT'].isin(['<2H','<12H'])]
    xdf.to_csv()
    xdf = xdf.replace(np.nan, 0)
    df3 = xdf.groupby(['DURCAT','EQUIPMENTKEY']).DURCAT.count()
    df4 = df3.to_frame (name='AB').reset_index ()
    df5 = df4[(df4.DURCAT == '<2H') & (df4['AB'] > 1)]
    df6 = df4[(df4.DURCAT == '<12H')]
    print(df6)
#df4['NW'] = df4.apply(lambda x: x.DURCAT + x.AB, axis = 1)
#df5 = df4[df4['NW'].isin(['<12H10','<2H2'])]
#print(df4, df4.columns, df4.shape[0])
#for i in range(len(df4)):
    #print(df4.loc[i, 'EQUIPMENTKEY'])



# In[ ]:





# In[ ]:




