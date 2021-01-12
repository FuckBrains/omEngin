#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import numpy as np

def str_manip():
    txt = "Hello, welcome to my world.8"
    x = txt.find(" ")  #find space at first occ
    y = txt.rfind(" ")	#find space at last occ
    ln = len(txt)	#find length of string
    cnt_space = txt.count(" ")  #find count of a string/char in string
    print(x)
    print(y)
    print(ln)
    print('count of space: ', cnt_space)
    print('return from char 5 to last', txt[5:]) 
    print('return from char 5 to first: ', txt[:5])
    print('return from char 6 to 10: ', txt[6:10])
    if 'to ' in txt:
        print('yes')
    else:
        print('no')
    print('convert lower case: ', txt.casefold())
    print('return true if end with xxx: ', txt.endswith('8'))
    print(txt.replace("world", "earth"))
    # isnumeric() , isalnum(), replace()

def fn_list(ls):
    #print(ls)
    lengt = len(ls)
    for index, value in enumerate(ls): 
        print(index)
        #print(value)
        #col = 0
        #print(value[col])
        #print(len(value[1]))
def fn_nparr(ar):
    #print(ar)
    #ar2df = pd.DataFrame(data=ar,columns=["c1", "c2","c3","c4","c5"])
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
            
def fn_dict(dc):
    #print(dc)
    #key_colname = 'Site_Code'
    #print(dc[key_colname])         #--printall values under keys/column site code
    #print(dc[key_colname][1])      #--print value in index 1
    #first_col_key = list(dc.keys())[0]
    #print(first_col_key)
    i = 0
    for txt in dc.keys():
        i = i + 1
        if "All_Tech" in txt:
            break

def mat_col(dic,srcstr):
    i = 0
    for txt in dic.keys():
        i = i + 1
        if srcstr in txt:
            break
    return i
        
filename = os.getcwd() + '//inc.csv'
df = pd.read_csv(filename)
arr = df.to_numpy()
dic = df.to_dict()
lst = df.values.tolist()
#print(df.head)
#fn_list(lst)  #--succ
#fn_dict(dic)  #--succ
#xx = mat_col(dic,"Site_Code")
#print(xx)
fn_nparr(arr)  #--succ


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




