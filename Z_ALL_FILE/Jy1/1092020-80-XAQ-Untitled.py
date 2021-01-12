#!/usr/bin/env python
# coding: utf-8

# In[8]:


import pandas as pd
import numpy as np
import os
import sqlite3

pt = os.getcwd()
alarm = pt + "\\C.csv"


def conv_2_list(ls1, ls2, ls3):
    ToDf = pd.DataFrame(zip(ls1, ls2, l3))
    print(Todf)
    

l0 = ["0", "1", "2", "3", "4"]
l1 = ["Amar", "Barsha", "Carlos", "Tanmay", "Misbah"] 
l2 = ["Alpha", "Bravo", "Charlie", "Tango", "Mike"] 
#conv_2_list(l0,l1,l2)


def concat(v1,v2):
    z = str(v1) + '-' + str(v2)
    return z

CDCT = lambda x : x[:4] if (len(x) >= 6) else "NF"

def df_add_col(dff,nwcol):
    df = dff.replace(r'^\s*$', np.NaN, regex=True)
    for i in range(len(df)):
        df.loc[i,nwcol] = concat(df.loc[i,"CUSTOMATTR15"],df.loc[i,"SUMMARY"])
    return df




df0 = pd.read_csv(alarm)
df1 = df0[['SERIAL','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','CUSTOMATTR3']]
x = df_add_col(df1,'scode')
ls = x.columns.to_list()
print(ls)
#print(x)





# In[ ]:





# In[ ]:




