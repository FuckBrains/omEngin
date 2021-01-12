#!/usr/bin/env python
# coding: utf-8

# In[73]:


import pandas as pd
import numpy as np
import os
from datetime import date
cols = ["SERIAL","EQUIPMENTKEY","CUSTOMATTR15","SUMMARY","LASTOCCURRENCE","CLEARTIMESTAMP","ALARMDETAILS"]
td = date.today()
pt = os.getcwd() + "\\" + "RRU_" + td.strftime('%Y-%m-%d') + ".csv"


TS2 = lambda y : ('NA' if (y is None) else y)

single = os.getcwd() + "\\" + "DWRRU.csv"
df = pd.read_csv(single)
df2 = df[cols] #pick customized column using list
df2['ABC'] = df2.apply(lambda x : TS2(df2['CUSTOMATTR15'].value_counts()),axis=1)

#code= [df2['CUSTOMATTR15'].value_counts(dropna=False)]
#ndf = pd.DataFrame(code).T  #list to dataframe
#ndf.to_csv(pt)



# In[ ]:





# In[ ]:




