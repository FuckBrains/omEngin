#!/usr/bin/env python
# coding: utf-8

# In[85]:


import pandas as pd
import numpy as np
from datetime import *
from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *
df = pd.DataFrame([{'a':12, 'b':'a', 'c':'Hello', 'ts':'2020-09-09 21:01:00'},
                         {'a':22, 'b':'a', 'c':'Hello1', 'ts':'2020-09-10 00:10:00'},
                         {'a':130, 'b':'a', 'c':'Hello2', 'ts':'2020-09-10 00:31:00'},
                         {'a':60, 'b':'b', 'c':'Hello3', 'ts':'2020-09-10 00:59:00'},
                         {'a':50, 'b':'b', 'c':'Hello4', 'ts':'2020-09-10 01:01:00'},
                         {'a':26, 'b':'b', 'c':'Hello5', 'ts':'2020-09-10 01:30:00'},
                         {'a':72, 'b':'c', 'c':'Hello6', 'ts':'2020-09-10 02:01:00'},
                         {'a':51, 'b':'b', 'c':'Hello4', 'ts':'2020-09-10 02:51:00'},
                         {'a':63, 'b':'b', 'c':'Hello5', 'ts':'2020-09-10 03:01:00'},
                         {'a':79, 'b':'c', 'c':'Hello6', 'ts':'2020-09-10 04:01:00'},
                         {'a':179, 'b':'c', 'c':'EVENT_3.5', 'ts':'2020-09-10 06:05:00'},
                         ])
df.ts = pd.to_datetime(df.ts)
df


# In[86]:


df.groupby(['ts'])['a']


# In[87]:


import os
df1 = pd.read_csv(os.getcwd() + "\\dt1.csv")


# In[88]:


df1


# In[140]:



def D():
    for index in range(df1.shape[1]):
        print('Column Number : ', index)
        columnSeriesObj = df1.iloc[: , index]
        print('Column Contents : ', columnSeriesObj.values)
    for (columnName, columnData) in df1.iteritems():
        print('Colunm Name : ', columnName)
        print('Column Contents : ', columnData.values)
    for (columnName, columnData) in empDfObj.iteritems():
        print('Colunm Name : ', columnName)
        print('Column Contents : ', columnData.values)


def C():
    for index, rows in df1.iterrows():
        print(rows)
        
def A():
    for i in range(len(df1)):
        print(df1.loc[i,:])
def B():
    for i in df1:
        print(df1.loc[:,i])

        
diffdate = lambda T1, T2 : (datetime.strptime(T2, "%d/%m/%Y %H:%M") - datetime.strptime(T1, "%d/%m/%Y %H:%M")).total_seconds()/60
diff_from_now = lambda locc : (datetime.now() - datetime.strptime(locc, "%d/%m/%Y %H:%M")).total_seconds()/60

def FN(x):
    print(type(x), x)
    if isinstance(x, np.int64):
        print("Y", x)
    
TS = lambda x : "A" if (isinstance(x, np.float64) == True) else (parse(str(x)).strftime("%d/%m/%Y %H:%M"))
    
for i in range(len(df1)):
    ls2 = list(map(lambda x: FN(x) , df1.loc[1].to_list()))

def A1():
    for i in range(len(df1)):
        ls2 = list(map(lambda x: TS(x) , df1.loc[1].values.tolist()))


#parse(x).strftime(fmt)
#ans = list(map(lambda y:y, ls1))
#print(ans)
    
#timestamp = [1545730073,1645733473]   # or timestamp = ['1545730073','1645733473']
#for ts in timestamp:
    #print(datetime.fromtimestamp(int(ts)).date())


# In[ ]:





# In[ ]:




