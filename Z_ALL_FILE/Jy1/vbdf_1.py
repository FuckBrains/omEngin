#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import os
import func.fnsem as sem
import func.fnfn as fn
import db.db as sq
from datetime import *

pt1 = os.getcwd() + "\\refdb\\S30.csv"
pt2 = os.getcwd() + "\\refdb\\S1800_200.csv"
pt3 = os.getcwd() + "\\refdb\\S1800.csv"


pt4 = os.getcwd() + "\\rocsites.csv"
despath = "C:\\Users\\kabir.omi\\Desktop\\radio_node_region_wise.csv"
df4 = pd.read_csv(pt3)
df4 = df4.rename(columns=str.upper)
df5 = sem.get_region(df4,'CUSTOMATTR15')
df = sem.catsemrw(df5)
print(df)

#Apply
def dtdiff(df,D1,D2):
    df[D1] = df[D1].map(lambda x: pd.to_datetime(x, errors='coerce'))
    df[D2] = df[D2].map(lambda x: pd.to_datetime(x, errors='coerce'))
    df['diff'] = df.apply(lambda x: abs(x[D1] - x[D2]), axis = 1)
    df['diff'] = df['diff'].astype('timedelta64[m]')
    print(df)
#applymap
dtdiff(df,'LASTOCCURRENCE','CLEARTIMESTAMP')

#map
#df['O1'] = df['CUSTOMATTR15'].map(lambda x: x[0:5])
#df['OM'] = df[list_of_cols_as_ref].apply(lambda x: ''.join(map(str,x)),axis=1)

#print(df)


#dfx = df5.groupby(['Region']).Region.count().to_frame(name = 'SMX').reset_index()
#dfx.to_csv(despath, index = False)
#dx2 = sem.get_region(dx1,dfdb)
#xx = fn.datediff(dx2,'cc','LASTOCCURRENCE','CLEARTIMESTAMP')
#print(xx)
#xx = fn.countifz(dx2,dx2['cat'])
#print(xx)
#cd = 'DHKDM36'
#idx = dx2[dx2['CUSTOMATTR15'] == cd].index
#print(dx2)
#dx4 = dx2[dx2['CUSTOMATTR15'].str.contains(cd)]
#dx3 = dx2[dx2['cat'].isin(['2G','3G','4G'])]
#dx4 = dx3.drop_duplicates(subset=['CUSTOMATTR15', 'cat'], keep='first')
#sem.techwise(dx4)


# In[3]:


# SERIES
# Ref: https://pandas.pydata.org/pandas-docs/stable/reference/series.html

def inlist(ls,str1,str2):
    n1 = ls.count(str1)
    n2 = ls.count(str2)
    n3 = str(n1) + str(n2)
    return n3

class srdf:
    def __init__(self, dfx):
        self.mdf = dfx
        self.df = dfx
    def getdf(self):
        return self.df
    def conv(self,sdf):
        print('x')
    def sscat(self,*argv):
        df1 = self.df
        n = len(argv) - 1
        print(n)
        i = 0
        ls = []
        idx = []
        ls = self.df.columns.to_list()
        while i < n :
            R = inlist(ls,argv[i],argv[i-1])
            if R[0:1] == '1' and R[1:2] == '1':
                print('11')
                self.df['OM'] = self.df.apply(lambda x: x[argv[i]] + x[argv[i+1]], axis = 1)
            if R[0:1] == '1' and R[1:2] == '0':
                print('10')
                self.df['OM'] = self.df.apply(lambda x: x[argv[i]] + argv[i+1], axis = 1)
            if R[0:1] == '0' and R[1:2] == '1':
                print('01')
                self.df['OM'] = self.df.apply(lambda x: argv[i] + x[argv[i+1]], axis = 1)
            i = i + 2
            df1 = self.df
        print(print(df1['OM']))
        
    

        
     
    
        #df['OM'] = df.apply(lambda x: x[argv[i]] + x[argv[i-1]], axis = 1)
        #df['OM'] = df.apply(lambda x: x[argv[i]] + "QASQ", axis = 1)
        #df['OM'] = df[argv[i],argv[i-1]].map(lambda x: x[0:5])
        #df['OM'] = df[argv[i]].str.cat(df[argv[i-1]], join='left')  
        #i -= 1
    #print(df)
    
        #for n in range(len(argv)):
        #if ls.count(argv[n]) != 0:
            #idx.append(str(n))
    #print(idx)
        
#ss = s.str.cat(t, join='left', na_rep='-')       
        
        
#x = srdf(df) 
#x.sscat('CUSTOMATTR15','cat','Region')
#print(x.getdf())

#s = pd.Series(['sca', 'bsc', np.nan, 'scd'])
#t = pd.Series(['d', 'a', 'e', 'c'])
#ss = s.str.cat(t, join='left', na_rep='-')


# In[ ]:





# In[ ]:





# In[ ]:




