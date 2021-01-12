#!/usr/bin/env python
# coding: utf-8

# In[25]:


import pandas as pd
import numpy as np
from datetime import *
from dateutil.parser import *
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


# In[26]:


df.groupby(['ts'])['a']


# In[27]:


import os
df1 = pd.read_csv(os.getcwd() + "\\dt1.csv")


# In[28]:


df1


# In[32]:



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
    
TS = lambda x : parse(str(x)).strftime("%Y%m%d%H%M%S") if isinstance(x, np.float64) == False and isinstance(x, np.int64) == False else "0"
TS1 = lambda x : parse(str(x)) if isinstance(x, np.float64) == False and isinstance(x, np.int64) == False else '1970-01-01 00:00:00'

#for i in range(len(df1)):
    #ls2 = list(map(lambda x: TS(x) , df1.loc[1].to_list()))
    #ls2 = list(map(lambda x: pd.to_datetime(TS1(x)).max , df1.loc[1].to_list()))
    #ls2 = list(map(lambda x: pd.to_datetime(TS1(x)).max , df1.loc[1].to_list()))
    #print('a')

def fun(arg):
    print(arg)
    return "A"

TX = lambda y : y.sort()

#df1['MAXC'] = df1.apply(lambda x : list(map(lambda y: pd.to_datetime(TS1(y)).max , x.loc[1].to_list()))], axis = 1)
#print(df1)
    
def ofn(ls):
    lss = []
    for i in range(len(ls)):
        if isinstance(ls[i], np.float64) == False and isinstance(ls[i], np.int64) == False:
            lss.append(parse(str(ls[i])))
    else:
        return max(lss)
    


import pandas as pd
import numpy as np
from datetime import *
from dateutil.parser import *

def find_lastest_date(dataframe):
    lss = []
    max_date = []
    df = dataframe.astype(str)
    for row in range(len(df)):
        for col in df:
            try:
                lss.append(parse(str(df.loc[row,col])))
            except:
                pass
        try:
            max_date.append(min(lss).strftime("%d/%m/%Y %H:%M"))  #change format for output column
        except:
            max_date.append("could not parse date from string")
    else:
        return dataframe.assign(lastest_date = np.array(max_date))
                

        
mydf = pd.DataFrame([
        ['Iphone','11-04-2020 12:14','11-09-2020 12:14','11-20-2020 12:24','400'],
        ['Iphone','CGHTZ09','11-09-2020 12:14','11-20-2020 12:24','400'],
        ['dell','11-05-2020 12:14','11-09-2020 12:14','11-20-2020 12:24','300'],
        ['dell','11-07-2020 12:14', '11-09-2020 12:13','11-20-2020 12:24','300'],
        ['Samsung ','12-09-2020 12:14', '11-09-2020 12:12','11-20-2020 12:24','250'],])

print(find_max_date(df1)) #change df1 to your



#parse(x).strftime(fmt)
#ans = list(map(lambda y:y, ls1))
#print(ans)
    
#timestamp = [1545730073,1645733473]   # or timestamp = ['1545730073','1645733473']
#for ts in timestamp:
    #print(datetime.fromtimestamp(int(ts)).date())


# In[ ]:





# In[ ]:





# In[ ]:




