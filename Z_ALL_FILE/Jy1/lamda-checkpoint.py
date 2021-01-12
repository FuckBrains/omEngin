#!/usr/bin/env python
# coding: utf-8

# In[13]:


import pandas as pd
import numpy as np
import os

cols = ["SERIAL","EQUIPMENTKEY","CUSTOMATTR15","SUMMARY","LASTOCCURRENCE","CLEARTIMESTAMP","ALARMDETAILS"]
single = os.getcwd() + "\\" + "DWRRU.csv"
df = pd.read_csv(single)
df2 = df[cols]
df2.fillna(0)
code= [df2['CUSTOMATTR15'].value_counts(dropna=False)]
print(code)
#df3 = df2.replace(np.nan,0)

#df2['count'] = codecount
#df2 = df2.replace(np.nan,0)
#print(df2)
#pt = os.getcwd() + "\\" + "DW1709.csv"
#df2.to_csv(pt)


# In[ ]:




