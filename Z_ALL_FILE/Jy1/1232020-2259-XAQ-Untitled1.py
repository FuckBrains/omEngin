#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import MySQLdb, os, pyodbc
from datetime import *
from dateutil.relativedelta import *
import numpy as np
from fn import *
import fluctuation as fl


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

pt = os.getcwd() + "\\"
df = pd.read_csv(pt + 'SEMQRY.csv')
#print(df)
#df = semqry()
#df = df.astype (str)
#print(df.dtypes)
#xxx = main(df)
df = df.astype (str)
df1 = fl.catmap_mod(df)
df1.columns
df2 = filter_p(df1, ['2G', '3G', '4G'], 'CATX')
df3 = filter_p(df1, ['other'], 'CATX')


# In[ ]:





# In[4]:


df2


# In[3]:


df3


# In[5]:


df4 = filter_p(df2, ['other'], 'CAT')


# In[6]:


df4


# In[7]:


df2


# In[ ]:




