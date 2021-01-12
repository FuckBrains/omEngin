#!/usr/bin/env python
# coding: utf-8

# In[18]:


import pandas as pd
import numpy as np
import os


def fn_nparr(ar):
    #print(ar)
    ar2df = pd.DataFrame(data=ar,columns=["c1", "c2","c3","c4","c5"])
    #print(ar2df)
    print('num of rows: ', (ar).shape[0])
    print('num of column: ', (ar).shape[1])
    #print(ar[0][1])  #----Acessing value by index
    #print(ar[0][2])   #----Acessing value by index
    i = 0
    print(ar.size)
    while i < ar.shape[1]:
        print(ar[i][0])   #printing rows values for column 0
        i = i + 1
        if(i == ar.size):
            print(i)
            break    

    
    
dr = os.getcwd()
print(dr)
filename = os.getcwd() + '//inc.csv'
df = pd.read_csv(filename)
#arr = df.to_numpy()
#dic = df.to_dict()
#lst = df.values.tolist()
#fn_nparr(arr)
    
    
    

    
    


# In[14]:


cat_type1(lst)


# In[ ]:




