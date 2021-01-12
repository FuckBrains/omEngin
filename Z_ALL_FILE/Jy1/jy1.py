#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import numpy as np
import os


def myFun(arg1, *argv, **kwargs): 
    print ("First argument :", arg1) 
    for arg in argv:
        print("Next argument through *argv :", arg)

def oFn1(df, *argv, **kwargs):
    ls = []
    col = df.columns.to_list()
    for n in range(len(argv)):
        TempLs = df[argv[n]].values.tolist()
        if len(ls) == 0:
            ls = TempLs
        else:
            tls = [i + j for i, j in zip(ls, TempLs)]
            ls = tls
    ld = []
    for key,value in kwargs.items():
        if col.count(value) != 0:
            TmpLd = df[value].to_list()
            if len(ld) == 0:
                ld = TmpLd
            else:
                tld = [i + j for i, j in zip(ld, TmpLd)]
                ld = tld
        else:
            ar = np.full(df.shape[0], value)
            TmpLd = ar.tolist()
            if len(ld) == 0:
                ld = TmpLd
            else:
                tld = [i + j for i, j in zip(ld, TmpLd)]
                ld = tld
    fls = []
    for i in range(len(ld)):
        x = ls.count(ld[i])
        fls.append(x)
    colx = 'C' + str(df.shape[1])
    df[colx] = np.array(fls)
    return df
        
db = os.getcwd() + "\\OMDB.csv"
livedb = os.getcwd() + "\\robi_live.csv"
xa = os.getcwd() + "\\xa.csv"
sclick = os.getcwd() + "\\OMTX_2.csv"
df = pd.read_csv(sclick)
ls_sclick = ['Severity','Node','Resource']
dc_sclick = {'Severity': 'Severity','Node':'Node','Resource':'Resource'}
#oFn1(df,'Severity','CustomAttr15', Severity = 'Critical', CustomAttr15 = 'CustomAttr15')


#df1 = df.groupby(['CustomAttr15','Summary'])['Summary'].count()
#print(df1)


# In[8]:


TS = lambda x : '2G' if ('2G SITE DOWN' in x)                 else ('3G' if ('3G SITE DOWN' in x)                 else ('4G' if ('4G SITE DOWN' in x)                 else ('Cell_2G' if ('2G CELL DOWN' in x)                 else ('Cell_3G' if ('3G CELL DOWN' in x)                 else ('Cell_4G' if ('4G CELL DOWN' in x)                 else "other")))))

dfdb1 = pd.read_csv(db)
dfdb = dfdb1[['Code','Zone']]
mdf = df.rename(columns=str.upper)
df1 = df.assign (coln='cat')
df1 = df1.assign (coln='Code')
df1['cat'] = df1.apply(lambda x: TS(x.SUMMARY), axis = 1)
df1['Code'] = df1.apply(lambda x: x.CUSTOMATTR15[0:5], axis = 1)
df2 = df1.merge(dfdb, on='Code')


#dfg = df2.groupby(['Zone','cat'])[''].count().to_frame (name='AB').reset_index ()
#dfg2 = df2.pivot_table(values='cat', index = 'Zone', columns = 'CustomAttr15')
#print(dfg2)
dfg1 = df2.groupby(['Zone','cat']).cat.count()
#print(dfg1.unstack(),chr(10),"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

getgrp = df2.groupby(['cat','Zone'])['CUSTOMATTR15'].get_group(('2G','BAR'))
print(getgrp)

#d = {'2G SITE':'ERI-2G SITE DOWN', '3G SITE':'ERI-3G SITE DOWN'}
#print(getList(d)) 
#for key, value in d.items() if 'new york' in key.upper():
#print(value)


# In[ ]:


dfT = pd.DataFrame({'x': ['A', 'B', 'A', 'B'], 'y': [1, 4, 3, 2]})
data_frame['Name'] = data_frame['Name'].apply(lambda name : name.upper())


# In[8]:


dfT.groupby(['x']).get_group('A')


# In[ ]:





# In[ ]:




