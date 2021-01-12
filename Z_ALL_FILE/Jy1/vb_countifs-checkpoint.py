#!/usr/bin/env python
# coding: utf-8

# In[59]:


import pandas as pd
import numpy as np
import os
from datetime import date
cols = ["Resource","CustomAttr15","Summary","LastOccurrence","CustomAttr11"] #Range [A-E]
single = os.getcwd() + "\\" + "single.csv"
df = pd.read_csv(single)
Rn = df[cols]
#print(RnA2E)

for column in Rn[['CustomAttr15']]:
    colseries = Rn[column]
    #print(colseries.values)

#for (colname, coldata) in RnA2E.iteritems():
    #print(colname)
    #print(coldata.values)
    
for colname in Rn[['CustomAttr15','Summary','LastOccurrence']]:
    colseries = Rn[column]
    #print(column)
    #print(colseries.values)
    
for col in range(Rn.shape[1]):
    #print('Column Number : ', col)
    ColSeries = Rn.iloc[: , col]
    for rw in range(Rn.shape[0]):
        rwval = Rn.iloc[rw , col]
        #print(rwval)

for rw in range(Rn.shape[0]):
    rwval = Rn.iloc[rw , '1']
    print(rwval)
    
#df2['CNT'] = df2.fillna(df2['CustomAttr15'].value_counts())
#print(df2)

#ar = collections.Counter(a)
#df2['newcol'] = arr.toarray().tolist() * 1
#print(ar)



#df2 = df.apply(lambda x: x['Col2'] if pd.isnull(x['Col1']) else x['Col1'], axis=1)


# In[ ]:




