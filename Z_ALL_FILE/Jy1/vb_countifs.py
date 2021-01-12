#!/usr/bin/env python
# coding: utf-8

# In[75]:


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
    ColSeries = Rn.iloc[: ,col]
    #print(ColSeries)

count = 1
for rw in range(Rn.shape[0]):
    rwvalue1 = Rn.iloc[rw , 1]  #on Column 1
    rwvalue2 = Rn.iloc[rw , 2]  ##on Column 2
    rwvalue3 = Rn.iloc[rw , 3]  ##on Column 2
    count = count + 1
    print('Row Number', count , ':', rwvalue1, '>', rwvalue2, '>', rwvalue3)
    if rwvalue1 == 'CXTKN25':
        print('x')
        print('CXTKN25 found at row:', str(count), ' Alamrm found: ' , rwvalue3)
    
#df2['CNT'] = df2.fillna(df2['CustomAttr15'].value_counts())
#print(df2)

#ar = collections.Counter(a)
#df2['newcol'] = arr.toarray().tolist() * 1
#print(ar)



#df2 = df.apply(lambda x: x['Col2'] if pd.isnull(x['Col1']) else x['Col1'], axis=1)


# In[ ]:





# In[ ]:




