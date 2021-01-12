#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import os
import numpy
import MySQLdb
import omdtfn as odt

#conn= MySQLdb.connect("localhost","root","admin","omdb")
#df_mysql = pd.read_sql("select * from sitedb",conn)
omdb = os.getcwd() + "\\" + "OMDB.csv"
pntxt = os.getcwd() + "\\" + "Periodic_Notification.txt"
pth = os.getcwd() + "\\" + "WRT1.csv"
pth2 = os.getcwd() + "\\" + "WRT2.csv"

#lambda <args> : <return Value> if <condition > ( <return value > if <condition> else <return value>)
TS = lambda x : '2G' if ('2G SITE DOWN' in x)                 else ('3G' if ('3G SITE DOWN' in x)                 else ('4G' if ('4G SITE DOWN' in x)                 else ('MF' if ('MAIN' in x)                 else ('DC' if ('VOLTAGE' in x)                 else ('TM' if ('TEMPERATURE' in x)                 else ('SM' if ('SMOKE' in x)                 else ('GN' if ('GEN' in x)                 else ('GN' if ('GENSET' in x)                 else ('TH' if ('THEFT' in x)                 else ('2_CELL' if ('2G CELL DOWN' in x)                 else ('3_CELL' if ('3G CELL DOWN' in x)                 else ('4_CELL' if ('4G CELL DOWN' in x)                 else "NA"))))))))))))

def write2txt(flname,txt):
    fo = open(flname,"w+")
    txt = fo.write(txt)
    fo.close()

class omdf:
    def __init__(self,dic):
        self.df = pd.DataFrame(dic)
        self.arr = self.df.to_numpy()
    def df_addcol_lamda(self):
        self.df['cat'] = self.df.apply(lambda row: TS(row.Summary), axis = 1)
        return self.df.to_dict()
    def df_addcol_fdic(self,d,newcolname):
        self.df[newcolname] = self.df['scode'].map(d)
        return self.df.to_dict()
    def df_apply_on_col(self,newcolname):
        self.df[newcolname] = self.df.apply(lambda x : x.CustomAttr15[0:5], axis = 1)
        return self.df.to_dict()
    def df_remove_col_by_list(self,lis):
        ndf = self.df[lis]
        return ndf.to_dict()


cols = ["SERIAL","EQUIPMENTKEY","CUSTOMATTR15","SUMMARY","LASTOCCURRENCE","CLEARTIMESTAMP","ALARMDETAILS","CUSTOMATTR15"]
single = os.getcwd() + "\\" + "DWRRU.csv"
df = pd.read_csv(single)
df2 = df[cols]
df2['count'] = df2['CUSTOMATTR15'].value_counts()
print(df2)
#df3 = df2.replace(np.nan,0)
#print(df2)



codelist = [df['CUSTOMATTR15'].to_list()]
print(codelist)



#Codelist = df2['CUSTOMATTR15']


#df2['cnt'] = df2['CUSTOMATTR15'].value_counts()
#print(df2)


#df2['cnt'] = lambda x : x.df2['CUSTOMATTR15'].value_counts()
#df['count'] = df['CUSTOMATTR15'].value_counts()
#print(df)
#print(df2)

#print(fdf['CUSTOMATTR15'].value_counts())
#df3 = df2.apply(lambda s: s['CUSTOMATTR15'], axis=1)
#df4 = df['CUSTOMATTR15'].value_counts().loc[lambda x : ]


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




