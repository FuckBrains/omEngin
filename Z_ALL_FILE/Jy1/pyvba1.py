#!/usr/bin/env python
# coding: utf-8

# In[7]:


import MySQLdb
import pandas as pd




def print_row_bynum(ar,rwnum,li):
    lcol = (ar).shape[1]
    j = 0
    heap = ""
    while j < lcol:
        hd = str(li[j]) + ":" + str(ar[rwnum][j])
        if j == 0:
            heap = hd
        else:
            heap = heap + '/n' + hd
        j = j + 1
    
    print(heap)
    return heap

def vba_match(ar,src):
    lrw = (ar).shape[0]
    lcol = (ar).shape[1]
    i = 0
    while i < lrw:
        if src == ar[i][0]:
            break
        i = i + 1
    return i

def fn_parse(ar,src,colnum):
    lrw = (ar).shape[0]
    lcol = (ar).shape[1]
    i = 0
    while i < lrw:
        if src == ar[i][0]:
            break
        i = i + 1
    return i



   







conn= MySQLdb.connect("localhost","root","admin","omdb")
df = pd.read_sql("select * from sitedb",conn)
#print(df.head())
ls = ['Site_Code', 'DG_Status','Revenue_(in_K_BDT)','Priority']
x = vba_match(df,'DHSDR19')
print(x)
#print(df2)
#lst = df2.columns.tolist()
#arr = df2.to_numpy()
#fn_byrw(arr,50,lst)
#print(vba_match(arr,'DHGUL19',1))
    
    


# In[ ]:





# In[ ]:




