#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd
import numpy as np
import os
from datetime import date
cols = ["Resource","CustomAttr15","Summary","LastOccurrence","CustomAttr11"] #Range [A-E]
single = os.getcwd() + "\\" + "single.csv"
print(single)
df = pd.read_csv(single)
Rn = df[cols]
#print(Rn)
count = 1

TS = lambda x : '2G' if ('2G SITE DOWN' in x)                 else ('3G' if ('3G SITE DOWN' in x)                 else ('4G' if ('4G SITE DOWN' in x)                 else ('MF' if ('MAIN' in x)                 else ('DC' if ('VOLTAGE' in x)                 else ('TM' if ('TEMPERATURE' in x)                 else ('SM' if ('SMOKE' in x)                 else ('GN' if ('GEN' in x)                 else ('GN' if ('GENSET' in x)                 else ('TH' if ('THEFT' in x)                 else ('2_CELL' if ('2G CELL DOWN' in x)                 else ('3_CELL' if ('3G CELL DOWN' in x)                 else ('4_CELL' if ('4G CELL DOWN' in x)                 else "NA"))))))))))))

def loop(Rn):
    for rw in range(Rn.shape[0]):
        rwvalue1 = Rn.iloc[rw , 1]  #on Column 1
        rwvalue2 = Rn.iloc[rw , 2]  ##on Column 2
        rwvalue3 = Rn.iloc[rw , 3]  ##on Column 3
        count = count + 1
        print('Row Number', count , ':', rwvalue1, '>', rwvalue2, '>', rwvalue3)
    for column in Rn[['CustomAttr15']]:
        colseries = Rn[column]
        print(colseries.values) #Transposed value of Column
    for (colname, coldata) in RnA2E.iteritems():
        print(colname) #Column Name
        print(coldata.values) #Transposed value of Column
        print('end')

class omdf:
    def __init__(self,dff):
        self.df = dff
        self.arr = self.df.to_numpy()
    def df_add_col_instr(self):
        self.df['cat'] = self.df.apply(lambda row: TS(row.Summary), axis = 1)
        return self.df.to_dict()
    def df_add_col_dic(self,colname,newcol,dic):
        self.df[newcol] = self.df['scode'].map(dic)
        return self.df.to_dict()
    def df_add_col_slice_str(self,newcolname):
        self.df[newcolname] = self.df.apply(lambda x : x.CustomAttr15[0:5], axis = 1)
        return self.df.to_dict()
    def df_rmv_column(self,lis):
        ndf = self.df[lis]
        return ndf.to_dict()
    def df_countif(self,column_name,newcolumn_name):
        code = pd.Series(self.df[column_name])
        lst = code.values.tolist()
        dic = {}
        for i in lst:
            dic[i] = lst.count(i)
        df_occ = pd.DataFrame(dic.items(),columns=[column_name, newcolumn_name])
        mdf = self.df.merge(df_occ, on=column_name)
        return mdf
    def df_instr(self,colname,srcstr):
        self.df[srcstr] = list(map(lambda x: x.count(srcstr), self.df[colname]))
        return self.df
    def df_vlookup(self,df2,common_colname):
        mdf = self.df.merge(df2, on=common_colname)
        return mdf

    
    #Rn['ABC'] = list(map(lambda x: x.count("CXTKN"), Rn['CustomAttr15']))
#print(Rn)
#ndf = countif('CustomAttr15','CountOf')

x = omdf(Rn)

#ndf = x.df_instr('CustomAttr15','DHSDR')
#print(ndf)


# In[ ]:





# In[191]:


L = lambda df,colname,dic : df[colname].map(dic)
dic = {'ERI-2G SITE DOWN':'2G','ERI-3G SITE DOWN':'3G'}
#dv = [value for key, value in dic.items() if '2G SITE DOWN' in key]
print(L(Rn,'Summary',dic))


# In[ ]:




