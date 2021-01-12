#!/usr/bin/env python
# coding: utf-8

# In[7]:


import MySQLdb
import pandas as pd
import os

conn= MySQLdb.connect("localhost","root","admin","omdb")
df = pd.read_sql("select * from sitedb",conn)
file = os.getcwd() + "\\" + "sem_raw.csv"


def dic_df_parse(dic,zn,zn_colname,parsecol_1,parsecol_2,parsecol_3):
    hp = ""
    #count = 0
    nd = pd.DataFrame(dic)
    ndf = nd[nd[zn_colname].str.contains(zn, na=False)]
    for ind in ndf.index: 
        code = str(ndf[parsecol_1][ind])
        lo = str(ndf[parsecol_2][ind])
        resource = str(ndf[parsecol_3][ind])
        hp = hp + " \n"  + code + " || " + lo + " || " + resource
    z = zn + ': \n' + hp
    return z

             
def fn_dict(dc,colname):
    #print(dc)
    #key_colname = 'Site_Code'
    #print(dc[colname])         #--printall values under keys/column site code
    
    print(ndf)
    #print(dc[key_colname][1])      #--print value in index 1
    #first_col_key = list(dc.keys())[0]
    #print(first_col_key)
    #i = 0
    #for txt in dc.keys():
        #i = i + 1
        #if "All_Tech" in txt:
            #break


#df2 = df[['Site_Code', 'DG_Status','Revenue_(in_K_BDT)','Priority']]
#dic = df2.to_dict()
#fn_dict(dic,'Site_Code')


dfc = pd.read_csv(file)
dic = dfc.to_dict()
gval = dic_df_parse(dic,'DHKTL04','CustomAttr15','Resource','Summary','LastOccurrence')
print(gval)

#dic_df_parse(dic,'DHKTL03','Resource','Resource','Summary','LastOccurrence',"","")


#zn,zn_colname,parsecol_1,parsecol_2,parsecol_3,cond1,cond1_colname

#for index, row in dfc.head().iterrows():
    #print(index, row['Resource'], row['Summary'], row['LastOccurance']) # access data using column names

#for index, row in dfc.head(4).iterrows():
     #print(row)



# In[ ]:





# In[ ]:




