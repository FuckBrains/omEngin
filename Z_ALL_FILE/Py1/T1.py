import pandas as pd
from datetime import *
import os
import TD1 as td

class omidf:
    def __init__(self, dfx):
        self.df = dfx
    def DateTime(self, ndf = pd.DataFrame([])):
        if len(ndf) <= 1:
            print(self.df)
        else:
            print(ndf)

def conv_lst_dic(lsKy, lsVal):
    try:
        dc = dict (zip (lsKy, lsVal))
        return dc
    except:
        print ('err')

def timedelt(diff):
    x = datetime.now ()
    d = x + timedelta (minutes=diff)
    str_d = d.strftime ("%d-%m-%Y %H:%M:%S")
    return str_d

#db = pd.read_csv(os.getcwd() + "\\OMDB.csv")
df = pd.read_csv(os.getcwd() + "\\csv\\DT.csv")
#df = df.rename(columns={'CUSTOMATTR15':'Code'})
#print(df)
df.set_index('LASTOCCURRENCE')['16/11/2020':'30/11/2020'].head()
print(df)
#pickcols()
#df = pd.read_csv(os.getcwd() + "\\csv\\TIME_TEST.csv", low_memory=False)
#df = df.astype(str)
#print(df.columns)








#df1 = dfdiff(df,'LASTOCCURRENCE')
#df1 = datetime_convert_format(df,'CLEARTIMESTAMP')
#df2 = datetime_convert_format(df1,'CLEARTIMESTAMP',"%d/%m/%Y %H:%M:%S")
