#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import os
import MySQLdb
import csv

conn= MySQLdb.connect("23.152.224.49","akomi","1q2w3eaz$","omdb")

pt = os.getcwd()
proxy = pt + '\\hideme.csv'
db = pt + '\\ip2as.csv'

def add_col(dA,c):
    rw, col = dA.shape
    lst = []
    for i in range(rw):
        x = dA[i][c]
        y = x.rfind('.')
        s = x[0:y]
        lst.append(s)
    dA = np.append(dA, np.array([lst]).transpose(), axis=1)
    return dA

dpx = pd.read_csv(proxy,delimiter=';')
ddb = pd.read_csv(db)
dpx1 = dpx[['ip','port']]
dA = dpx1.to_numpy()
z = add_col(dA,0)
df1 = pd.DataFrame(z,columns=['ip','port','IPMOD'])
df = pd.read_sql("select * from ipasn10",conn)
fdf = df1.merge(df, on='IPMOD')
fdf.to_csv(pt + '\\merged.csv')
print(fdf)


# In[ ]:




