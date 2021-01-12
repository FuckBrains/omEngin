#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[2]:


import pandas as pd
import numpy as np
import os
import MySQLdb
import csv

conn= MySQLdb.connect("23.152.224.49","akomi","1q2w3eaz$","omdb")

pt = os.getcwd()
proxy = pt + '\\hideme.csv'
db = pt + '\\ip2as.csv'

def add_col(dA,c):
    rw, col = dA.shape
    lst = []
    for i in range(rw):
        x = dA[i][c]
        y = x.rfind('.')
        s = x[0:y]
        lst.append(s)
    dA = np.append(dA, np.array([lst]).transpose(), axis=1)
    return dA

dpx = pd.read_csv(proxy,delimiter=';')
ddb = pd.read_csv(db)
dpx1 = dpx[['ip','port']]
dA = dpx1.to_numpy()
#z = add_col(dA)
#df1 = pd.DataFrame(z,columns=['ip','port','ipmod'])

dd = ddb.to_numpy()
z1 = add_col(dd,0)
df2 = pd.DataFrame(z1,columns=['IP1','IP2','ASN','Country','ISP','IPMOD'])
df = df2.to_csv(pt + '\\A2S.csv',',')


# In[ ]:


cursor = conn.cursor()
try:
    with open(pt + '\\A2S.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        sql = "INSERT INTO ipasn3 (IP1,IP2,ASN,Country,ISP,IPMOD) VALUES (%s,%s,%s,%s,%s)"
        for row in csv_reader:
            row = (', '.join(row))
            print(row)
            cursor.execute(sql, row)
except:
    conn.rollback()
finally:
    conn.close()


# In[77]:


list_of_dates = ['2019-11-20', '2020-01-02', '2020-02-05','2020-03-10','2020-04-16','2020-05-01']
employees = ['Hisila', 'Shristi','Zeppy','Alina','Jerry','Kevin']
salary = [200,400,300,500,600,300]
df = pd.DataFrame({"Name":employees,'Joined date': pd.to_datetime(list_of_dates),"Salary":salary})
df['Status'] = ["Senior" if s >=400 else "Junior" for s in df['Salary']] 
#print(df)

df['Status'] = np.where(df['Salary']>=400, 'Senior', 'Junior')
nm = df.to_numpy()
rw, col = nm.shape
print(nm)

ml = []
for i in range(rw):
    if nm[i,2]>300:
        ml.append('GT')
    else:
        ml.append('LT')


# In[78]:


empty_array = np.empty((4, 0), int)
print(empty_array)


# In[79]:


column_list_1 = [11, 21, 31, 41]
empty_array = np.append(empty_array, np.array([column_list_1]).transpose(), axis=1)
print(empty_array)


# In[80]:


column_list_2 = [15, 25, 35, 45]
empty_array = np.append(empty_array, np.array([column_list_2]).transpose(), axis=1)
print(empty_array)


# In[81]:


nm = np.append(nm, np.array([ml]).transpose(), axis=1)
print(nm)


# In[ ]:





# In[ ]:




