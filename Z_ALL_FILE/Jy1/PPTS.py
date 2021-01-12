#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[3]:


import pandas as pd
import MySQLdb, os, pyodbc
from datetime import *
from dateutil.relativedelta import *
import numpy as np
from fn import *

def PP(df):
    try:
        print(df['LASTOCCURRENCE', 'DUR', 'DURCAT'])
    except:
        try:
            print(df['LASTOCCURRENCE', 'DUR'])
        except:
            print(df['LASTOCCURRENCE'])
            
def series2df(sr1, sr2):
    df = pd.concat([sr1, sr2], axis=1)
    return df

def DateDiff(df, newcol, col1, col2 = False, DayFirst = True):
    if col2 == False:
        lscol = df[col1].to_list()
        ls = list(map (lambda x: ((datetime.now() - datetime.strptime(x, "%d/%m/%Y %H:%M")).total_seconds())/60, lscol))
        df[newcol] = np.array(ls)
    else:
        lscol1 = df[col1].to_list()
        lscol2 = df[col2].to_list()
        ls = list(map (lambda x , y: ((datetime.strptime(x, "%d/%m/%Y %H:%M") - datetime.strptime(y, "%d/%m/%Y %H:%M")).total_seconds())/60 if ('1970' not in str(y)) else "0", lscol2,lscol1))
        df[newcol] = np.array(ls)
    df[newcol] = df[newcol].astype(float).round(2)
    return df
    
def xxz(df):
    df['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].apply(lambda x : pd.Timestamp(x))
    return df

def Delta(df):
    df['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].apply(lambda x : x - pd.to_timedelta(2))
    print(xdf)
    
def Sr2Tstamp(df):
    xx['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].to_timestamp
    print(xx)

def DateTime_toSQL(df):
    df['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].apply(lambda x : pd.to_datetime(x, errors='coerce', dayfirst = True, cache=True).strftime("%Y/%m/%d %H:%M:%S"))
    return df
    

pt = os.getcwd() + "\\"
df = pd.read_csv(pt + 'P.csv')
#xd = DateTime(df)
#Delta(xd)
#Sr2Tstamp(df)
#xxz(df)
xa = DateDiff(df, "DUR", "LASTOCCURRENCE")
print(xa)



# In[ ]:





# In[ ]:




