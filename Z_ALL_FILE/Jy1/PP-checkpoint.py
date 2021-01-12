#!/usr/bin/env python
# coding: utf-8

# In[5]:


import time as tm
import os, cx_Oracle
from datetime import *
import numpy as np
import pandas as pd




pt = os.getcwd() + "\\book1.csv"
df = pd.read_csv(pt)
df = df.astype (str)
df = df.rename (columns=str.upper)
df1 = df[['SERIAL','SUMMARY','CUSTOMATTR15','CUSTOMATTR11','LASTOCCURRENCE']]
df1 = df1.assign(DHM ='0')
df1['DHM'] = df.apply(lambda x: pd.to_datetime(x['LASTOCCURRENCE'], dayfirst=True).strftime("%m%d%H%M"), axis = 1)
df1 = df1.sort_values(by=['DHM'], ascending=True)
df1 = df1.reset_index()
df1 = df1.assign(CAT5 ='0')
df1 = df1.assign(CAT15 ='0')


x = df1.shape[0]
df1['DHM'] = df1['DHM'].astype(int)
st = int(df1.loc[0,'DHM'])
precode = '0'
x1 = 0
for i in range(len(df1)):
    code = df1.loc[i,'CUSTOMATTR15']
    if x1 == 10:
        st = st + 5
        x1 = 0
    if int(df1.loc[i,'DHM']) > st:
        if precode != code:
            precode = code
            st = int(df1.loc[i,'DHM']) + 5
            df1.loc[i,'CAT5'] = st
        else:
            df1.loc[i,'CAT5'] = st
            x1 = x1 + 1
    else:
        df1.loc[i,'CAT5'] = st
        precode = code
        


# In[ ]:




