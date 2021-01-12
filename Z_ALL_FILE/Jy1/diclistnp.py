#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import os
from datetime import date
import itertools

cols = ["Resource","CustomAttr15","Summary","LastOccurrence","CustomAttr11"] #Range [A-E]
single = os.getcwd() + "\\" + "single.csv"
dff = pd.read_csv(single)
df = dff[cols]
print(df)


# ### Numpy  [from df, to dic, to list]

# In[ ]:


arr = df.to_numpy()  #convert df to np
print(arr[0][0])   #printing an index value of numpy arr
rw, col = arr.shape #last row, last column
print(rw,col)

#loop and access
lst = []
dic = {}
for i in range(rw):
    lst2 = []
    for j in range(col):
        #print(arr[i][j])      #print Row by index
        lst2.append(arr[i][j]) #create a list
    dic.update( {i : lst2} ) #create dict
#print(dic)


# ### Numpy [add col from list]

# In[ ]:


#add new column derived from existing one
lst3 = []
for i in range(rw):
    x = arr[i][2] #only printing summary
    if 'DOWN' in x:
        lst3.append('down')
    else:
        lst3.append('no')
arr = np.append(arr, np.array([lst3]).transpose(), axis=1)
df = pd.DataFrame(arr)
print(df)


# # List

# In[ ]:


#derived list from df
dff = pd.Series(df['CustomAttr15'])
mlst1 = dff.to_list()
mlst2 = df.values.tolist()
mlst3 = df.columns.values.tolist()
mlst4 = df['Summary'].values.tolist()
mlst5 = df[['Summary','LastOccurrence']].values.tolist()
#print(mlst4)

def lp_1d_list(mlst1):
    i = 0
    for i in range(len(mlst1)):
        print(mlst1[i])
        i = i + 1
def lp_nested_seperate_2_list(mlst1,mlst4):
    for a in mlst1:
        for b in mlst4:
            print(a,">",b)
def lp_nested_list(mlst2):
    for i in range(len(mlst2)):
        for j in range(len(mlst2[i])):
            print(mlst2[i][j])

# List Methods append(), count(), index(), pop(), sort()
fruits = ['apple', 'banana', 'cherry','banana']
fruits.append("orange")
print(fruits)
print(fruits.count("banana"))
print(fruits.index("cherry"))
fruits = ['apple', 'banana', 'cherry']
cars = ['Ford', 'BMW', 'Volvo']
fruits.extend(cars)
print(fruits) #JOIN 2 LIST
fruits = fruits.pop(1)
print(fruits)fruits.extend(cars)


# # dictionary

# In[ ]:


dic1 = {}
dic2 = {1: 'apple', 2: 'ball'}
dic3 = {'name': 'John', 1: [2, 4, 3]}
dic4 = dict({1:'apple', 2:'ball'})
dic5 = dict([(1,'apple'), (2,'ball')])

#create dictionary from 2 list (as key , as value)
dlist = dict(zip(mlst1, mlst5))
#print(dlist)

#dataframe to dictionary
ddf1 = df.to_dict()

def lp_dic(ddf1):
    for key in ddf1:
        print(key,ddf1[key])
    for v in ddf1.values():
        print(v)
def lp_key_wise(dl):
    for k,v in dlist.items():
        print("STCODE:", k, ":", v[0],',', v[1])
        
lp_key_wise(dlist)
#Method of Dictionary fromkeys(), get(), items(), keys(), values(), pop(), update()
person = {'name': 'Phill', 'age': 22}
#print(person.get('name'))

d = {1: "one", 2: "three"}
d1 = {2: "two"}
d.update(d1)
#print(d)

person = {'name': 'Phill'}
person.setdefault('age', 22)
#print(person)


# In[ ]:





# In[ ]:





# In[ ]:




