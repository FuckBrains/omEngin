#!/usr/bin/env python
# coding: utf-8

# ##### Create Dictionary

# In[4]:


dc1 = {'1': 'uno', '2': 'dos', '3': 'tres'}
dc2 = {'1': ['A','B'], '2': ['J','K'],'3':['X','Y']}
dc3 = {('a','b','c'),('x','y','z')}


# ##### Accessing Dic => Keys(), values(), Items() 

# In[5]:


print(dc2.keys())
print(dc2.values())
print(dc2.items())


# ##### merging 3 dictionary

# In[16]:


A = {'x': 10, 'y': 20}
B = {'y': 30, 'z': 40}
A.update(B)
A.update({'m':100,'n':200})
print(A)


# ##### Method => get()

# In[ ]:


print(A.get('x'))


# ##### create dictionary merging 2 list

# In[22]:


ls1 = ['1','2','3']
ls2 = ['a','b','c']
dc5 = {1: ls1, 2: ls2}
print(dc5)


# In[ ]:




