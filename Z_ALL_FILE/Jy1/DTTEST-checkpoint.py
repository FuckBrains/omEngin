#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import os
import MySQLdb
from datetime import *

db = os.getcwd() + "\\OMDB.csv"
semcol = os.getcwd() + "\\semcols.txt"
cat = os.getcwd() + "\\catdef.txt"
conn= MySQLdb.connect("localhost","root","admin","om2")

x = datetime.now()
y = datetime.strftime(x, "%m-%d-%Y %H:%M:%S")
print(y)
svpt = os.getcwd() + "\\OMDW.csv"
df = pd.read_csv(svpt)

df['LASTOCCURRENCE'] = pd.to_datetime(df['LASTOCCURRENCE'])
df['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].map(lambda x: x.strftime("%d/%m/%Y %H:%M:%S"))
df = df.assign(NW = y)
df['DUR'] = df.apply(lambda x : pd.to_datetime(x.NW) - pd.to_datetime(x.LASTOCCURRENCE) ,axis=1)
df['DUR'] = df['DUR'].astype('timedelta64[m]')
print(df)
#df['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].map(lambda x: x.strftime("%d/%m/%Y %H:%M:%S"))
#df = df.assign(NW = y)
#print(df.dtypes)
#df['DUR'] = pd.to_datetime(y - pd.to_datetime(df['LASTOCCURRENCE'])
#df['DUR'] = df.apply(lambda x : y - pd.to_datetime(x.LASTOCCURRENCE)) ,axis=1)


# In[27]:





# In[ ]:





# In[ ]:





# In[ ]:




