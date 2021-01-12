#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[2]:


import pandas as pd

def pr_all_row_dict(dic):
    for rw in dic.values():
        print(rw)

def pr_all_col_dict(dic):
    print(dic)(1)
    #for col in dic:
        #print(col)[0]

def pr_spfc_col_dict(dic,index):
    print(dic.values())
    print(dic.items())

dict1 = {'col0':('zero_1','zero_2'),'col1':'one', 2:'two'}
#pr_all_row_dict(dict1)
#pr_all_col_dict(dict1)
pr_spfc_col_dict(dict1,0)



#for col in dic.items():
    #print(col)

#for val in dic:
    #print(val)

    
#print(dic['Site_Code'])   #colname=sitecode
#index = 2
#print(dic['Site_Code'][index])   #colname=sitecode


# In[ ]:





# In[ ]:





# In[86]:


#df to dic
import pandas as pd
filename = 'RobiLive.csv'
df = pd.read_csv(filename)
dic = df.to_dict()
#print(df)
#print(dic)
#for col in dic:  #print header
    #print(col)
#for rw in dic.values():  #print header
    #print(rw)
#for col in dic.items():
    #print(dic['Site_Code'])
print(dic['Site_Code'])   #colname=sitecode
index = 2
print(dic['Site_Code'][index])   #colname=sitecode
#for i in dic.values():
    #j = j + 1
    #print(j)
   # print(dic['Site_Code'][j])
    
#for i in dic:
    #print(i)
#for i in dic.values():
    #print(i)


# In[ ]:





# In[ ]:





# In[ ]:




