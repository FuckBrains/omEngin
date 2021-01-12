import pandas as pd
import numpy as np
import os
import requests
import lookup.lookup as look
import func.fnstr as fst
import func.fndatetime as fdt
import func.fnlook as flk
import db.db as sq
import db.semqry as semq
import func.fnfn as fn
import prep as pr
import func.omdtfn as odt
import rruocc as rru
import top5 as t5
from datetime import *


df0 = semq.qry_all_last_day('SEMHEDB.ALERTS_STATUS','SOC_READ','soc_read',' * '," AND AGENT IN ('U2000 TX','Ericsson OSS','EricssonOSS','Huawei U2000 vEPC','Huawei U2020','LTE_BR1_5','MV36-PFM3-MIB','BusinessRule14','BusinessRule14_ERI_ABIP')")
df1 = df0.rename(columns=str.upper)
rru.theft_occ(df1)
t5.top5_outage_report(df1)
#df20 = df17[['CODE','CName','CNT','SMX']]
#df19 = df20.applymap(str)
#for i in range(len(df19)):
    #print(df19.loc[i,'CName'] + chr(10) + df19.loc[i,'CODE'] + ': ' + str(df19.loc[i,'CNT']) + '/' + str(df19.loc[i,'SMX']))
    #print(chr(10))
#df04 = pd.DataFrame(df03)
#print(df04)
#print(df3)
#df = pd.DataFrame(fruit_list, columns = ['Name' , 'Price', 'In_Stock'])
#x = df.groupby(df.Name == 'banana').Price.sum()
#print(x)
#def rru_occ(df):
#df = df0.rename(columns=str.upper)
#df = process_sem_data(df0)
#df1 = df[['CUSTOMATTR15','AGING','cat', 'ShortCode', 'Region']]
#df2 = fst.add_col_df(df1,'NEW_AG')
#df2['NEW_AG'] = df2.apply(lambda x : x.AGING/(60*24), axis = 1)
#df3 = df2.groupby(df2['ShortCode']).NEW_AG.sum()
#print(df2)
#df3 = df2.groupby(['Region','cat']).cat.count()
#print(df3)
#print(df3.first())
#df4 = pd.DataFrame(df3)
#print(df3)
