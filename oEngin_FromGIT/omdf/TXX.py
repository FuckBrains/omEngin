

import pandas as pd
import numpy as np
from datetime import *
import os


dfdb = pd.read_csv(db)
df = pd.read_sql('select * from big5', con = conn)
df0 = df.rename(columns=str.upper)
    ls = text2list(semcol)
    df1 = df0[ls]
    dc = text2dic(cat)
    df1['cat'] = df1.apply(lambda x: getkey(dc, x.SUMMARY) , axis = 1)
    df1['Code'] = df1.apply(lambda x: x.CUSTOMATTR15[0:5], axis = 1)
    df2 = df1.merge(dfdb, on='Code')
x = datetime.now()
y = datetime.strftime(x, "%m-%d-%Y %H:%M:%S")
svpt = os.getcwd() + "\\OMDW.csv"
df = pd.read_csv(svpt)
df['LASTOCCURRENCE'] = pd.to_datetime(df['LASTOCCURRENCE'])
df['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].map(lambda x: x.strftime("%d/%m/%Y %H:%M:%S"))
df = df.assign(NW = y)
df['DUR'] = df.apply(lambda x : pd.to_datetime(x.NW) - pd.to_datetime(x.LASTOCCURRENCE) ,axis=1)
df['DUR'] = df['DUR'].astype('timedelta64[m]')
print(df)