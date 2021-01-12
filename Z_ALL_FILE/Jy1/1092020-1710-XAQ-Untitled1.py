#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

df = pd.DataFrame({'a': [1, 1, 5, 7, 1, 5, 1, 4, 7, 8, 9],
                   'b': [3, 5, 6, 2, 4, 6, 7, 8, 7, 8, 9]})

#print(df['a'].to_list())
#print(df['a'].value_counts())

df['count'] = df['a'].value_counts()
#df2 = df.replace(np.nan,0)
print(df)


# In[ ]:





# In[ ]:





# In[ ]:




