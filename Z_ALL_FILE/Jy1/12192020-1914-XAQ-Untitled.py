#!/usr/bin/env python
# coding: utf-8

# In[54]:


import pandas as pd
import numpy as np
from datetime import *
import os
from fn import *
from oDT import *

print(os.getcwd() + "\\B1.csv")
#df1 = pd.read_csv(os.getcwd() + "\\book1.csv")
df = pd.read_csv(os.getcwd() + "\\B1.csv")
nw = datetime.now()



TS1 = lambda x: '2' if ('2G SITE DOWN' in x)     else ('2' if ('2G CELL DOWN' in x)     else ('3' if ('3G SITE DOWN' in x)     else ('3' if ('3G CELL DOWN' in x)     else ('4' if ('4G SITE DOWN' in x)     else ('4' if ('4G CELL DOWN' in x)     else ('2' if ('OML' in x)     else "0"))))))

TS2 = lambda x: '2' if ('2G SITE DOWN' in x)     else ('22' if ('2G CELL DOWN' in x)     else ('3' if ('3G SITE DOWN' in x)     else ('33' if ('3G CELL DOWN' in x)     else ('4' if ('4G SITE DOWN' in x)     else ('44' if ('4G CELL DOWN' in x)     else ('2' if ('OML' in x)     else "0"))))))


DCAT = lambda x: 'H2' if (x < 300) else ('H12')

def extrafeat(df, tmdelta = 0):
    df1 = df.astype(str)
    df1 = df1.rename (columns=str.upper)
    df1 = df1[~df1['CUSTOMATTR15'].isin(['UNKNOWN'])]
    df1 = df1.assign (CT1='X')
    df1 = df1.assign (CT2='X')
    df1['CT1'] = df1.apply (lambda x: TS1 (x.SUMMARY), axis=1)
    df1['CT2'] = df1.apply (lambda x: TS2 (x.SUMMARY), axis=1)
    df1 = df1[~df1['CT1'].isin(['0'])]
    df1['CT1_1'] = df1['CUSTOMATTR15'].map(str) + '_' + df1['CT1'].map(str)
    df1['CT1_2'] = df1['CUSTOMATTR15'].map(str) + '_' + df1['CT2'].map(str)
    try:
        df2 = DateDiff(df1, "DUR", "LASTOCCURRENCE")
    except:
        df2 = datediff_ondf(df1, "DUR", 'LASTOCCURRENCE')
    df2['DCT'] = df2.apply (lambda x: DCAT(x.DUR), axis=1)
    df2['LO'] = df2.apply (lambda x: pd.to_datetime (x['LASTOCCURRENCE'], errors='coerce', cache=True).strftime("%Y%m%d%H%M"), axis=1)
    df2 = df2.astype(str)
    df2['CD_TM_CT1'] = df2['CUSTOMATTR15'].map(str) + '_' + df2['LO'].map(str) + '_' + df2['CT1'].map(str)
    df2['CD_TM_CT2'] = df2['CUSTOMATTR15'].map(str) + '_' + df2['LO'].map(str) + '_' + df2['CT2'].map(str)
    df2.to_csv(os.getcwd() + "\\P3.csv", index = False)
    df3 = df2.drop_duplicates(subset=['CD_TM_CT2'], inplace=False, ignore_index=True)
    df3 = df3.reset_index()
    df4 = df3.drop_duplicates(subset=['CD_TM_CT1'], inplace=False, ignore_index=True)
    df4 = df4.reset_index()
    df4.to_csv(os.getcwd() + "\\P5.csv", index = False)
    return df4

def Part2(df):
    dfx = df.groupby(['CT1_2','DCT']).CT1_2.count().to_frame(name = 'FC').reset_index()
    #df.to_csv(os.getcwd() + "\\P6.csv", index = False)
    pv = dfx.pivot_table(index=['CT1_2'], columns='DCT', values='FC', aggfunc='sum').reset_index()
    df = pv.drop_duplicates(subset=['CT1_2'], inplace=False, ignore_index=True)
    pv.to_csv(os.getcwd() + "\\"IAMPY".csv", index = False)
    #df['H12'] = df['H12'].fillna(0, inplace=True)
    #df['H2'] = df['H2'].fillna(0, inplace=True)
    print(df)
    
def pvt(df):
    pv = df.pivot_table(index=['CT1_2','DCT'], columns='DCT', values='CT1_2', aggfunc='sum').reset_index()
    print(pv)
    

#pvt = fdf.pivot_table(index=['CUSTOMATTR15','CAT'], columns='DURCAT', values='cnt', aggfunc='sum').reset_index()

fdf = extrafeat(df)
Part2(fdf)
#pvt(fdf)


# In[ ]:





# In[ ]:




