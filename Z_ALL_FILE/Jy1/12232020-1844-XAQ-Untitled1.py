#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
from saymyname import *
from oDT import *
import os

def cols():
    fp = open(os.getcwd() + "\\csv\\col.txt")
    tx = fp.read()
    fp.close()
    return tx.split("\n")

def csv_mini():
    ls = cols()
    df = pd.read_csv(os.getcwd() + "\\csv\\OMTX1.csv", low_memory=False)
    df1 = df[ls]
    df1.to_csv(os.getcwd() + "\\csv\\OMTX_M.csv", index = False)

    
df = pd.read_csv(os.getcwd() + "\\csv\\OMTX_M.csv", low_memory=False)
print(df)


# In[ ]:





# In[ ]:




