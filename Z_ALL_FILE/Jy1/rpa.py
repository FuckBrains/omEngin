#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import numpy as np
import os
from datetime import *
from fn import *


def getvalue(my_dict, ky):
    if ky is not None:
        for key, value in my_dict.items ():
            if key in str (ky):
                return value
        else:
            return 0

TS = lambda x : '2G' if ('2G SITE DOWN' in x)                 else ('3G' if ('3G SITE DOWN' in x)                 else ('4G' if ('4G SITE DOWN' in x)                 else ('MF' if ('MAIN' in x)                 else ('DL' if ('VOLTAGE' in x)                 else ('TM' if ('TEMPERATURE' in x)                 else ('SM' if ('SMOKE' in x)                 else ('GN' if ('GEN' in x)                 else ('GN' if ('GENSET' in x)                 else ('TH' if ('THEFT' in x)                 else ('C2G' if ('2G CELL DOWN' in x)                 else ('C3G' if ('3G CELL DOWN' in x)                 else ('C4G' if ('4G CELL DOWN' in x)                 else ('DOOR' if ('DOOR' in x)                 else "NA")))))))))))))

dfd = pd.read_csv (os.getcwd() + "\\OMDB.csv")
lss = dfd['sCode'].to_list()

def codecorr(code,akey):
    cd = code
    if 'UNKNOW' in code:
        for i in range(len(lss)):
            vl = akey.find(lss[i])
            if vl > 0 and vl is not None:
                cd = akey[vl:vl+7]
                break
        else:
            return cd
    else:
        return cd

def msgprep_head_znwise(hd = "Periodic Notification"):
    nw = datetime.now()
    dt = nw.strftime("%d-%m-%Y")
    tm = nw.strftime("%H:%M")
    a1 = hd + " at " + tm + " on " + dt
    return a1

def catmap_mod(df,dfdb):
    df0 = df.rename (columns=str.upper)
    ls = ['NODE','RESOURCE','CUSTOMATTR15','SUMMARY','ALERTKEY','LASTOCCURRENCE']
    df1 = df0[ls]
    df1 = df1.assign(CAT = df1.apply (lambda x: TS (x.SUMMARY), axis=1))
    df1 = df1.assign(CODE = df1.apply (lambda x: codecorr(x.CUSTOMATTR15, x.ALERTKEY), axis=1))
    df2 = df1.assign(sCode = df1.apply (lambda x: x.CODE[0:5] if (x.CODE is not None) else "XXXXXXXX", axis=1))
    df3 = df2.merge (dfdb, on='sCode')
    df3['CODECAT'] = df3['CUSTOMATTR15'].str.cat(df3['CAT'])
    df3['ZNCAT'] = df3['sZone'].str.cat(df3['CAT'])
    #print(df3[['CODECAT','ZNCAT']])
    return df3

def zonewise_count(df0, oncat=[]):
    zn = ['DHK_M','DHK_N','DHK_O','DHK_S','CTG_M','CTG_N','CTG_S','COM','NOA','BAR','KHL','KUS','MYM','RAJ','RANG','SYL']
    Tcnt = {}
    hd1 = ""
    hd2 = ""
    for j in range(len(oncat)):
        Tcnt[oncat[j]] = 0
        if hd1 == "":
            hd1 = "Region: " + oncat[j]
        else:
            hd1 = hd1 + "/" + oncat[j]
    hp = chr(10)
    ls = []
    for i in range(len(zn)):
        for n in range(len(oncat)):
            zct  = zn[i] + oncat[n]
            cnt = countifs(df0,df0['ZNCAT'],zct)
            ls.append(str(cnt))
            current_val = int(getvalue(Tcnt, oncat[n])) + cnt
            Tcnt[oncat[n]] = current_val
        else:
            hp = hp + chr(10) + zn[i] + ": " + "/".join(list(ls))
            ls = []
    for k in range(len(oncat)):
        hdval = Tcnt.get(oncat[k])
        if hd2 == "":
            hd2 = "National: " + str(hdval)
        else:
            hd2 = hd2 + "/" + str(hdval)
    else:
        trail1 = "This is RPA generated periodic notification." + chr(10) + "For any query, please contact with " + chr(10)
        trail2 = trail1 + "SOC Shift Manager, 01817183680"
        FinalText = msgprep_head_znwise() + chr(10) + chr(10) + hd1 + chr(10) + hd2 + hp + chr(10) + chr(10) + trail2
        return FinalText


#sclick.csv must have Required column : 'NODE','RESOURCE','CUSTOMATTR15','SUMMARY','ALERTKEY','LASTOCCURRENCE'
df = pd.read_csv(os.getcwd() + "\\sclick.csv")  # data source, 
dfdb = pd.read_csv (os.getcwd() + "\\OMDB.csv") #fixed data in same folder
xx = catmap_mod(df,dfdb) # function is for processing data
ST = zonewise_count(xx, ['2G','3G','4G'])  #2G,3G,4G derived from lambda function "TS". check above
print(ST)




# In[ ]:





# In[ ]:




