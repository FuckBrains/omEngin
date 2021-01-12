import pandas as pd
import numpy as np
import os
import func.fndatetime as fdt
import func.fnlook as flk
import func.fnstr as fst
import db.db as sq
from datetime import *


pt1 = os.getcwd() + "\\refdb\\S30.csv"
pt2 = os.getcwd() + "\\refdb\\S1800_200.csv"

df1 = pd.read_csv(pt1)
df0 = pd.read_csv(pt2)

def get_region(df):
    df4 = df
    df5 = flk.add_col_df(df4,'ShortCode')
    df5['ShortCode'] = df5.apply(lambda x : x.CUSTOMATTR15[0:5], axis = 1)
    cols = "ShortCode,Region"
    dfdb = sq.omdb(cols)
    df6 = flk.vlookup(df5,dfdb,'ShortCode','NA')
    df6.drop('ShortCode', axis='columns', inplace=True)
    return df6

def conv_to_datetime(df1,col):
    df1[col] = pd.to_datetime(df1[col], errors='coerce')
    return df1

def pick_by_day(df1,day):
    df2 = df1[df1['LASTOCCURRENCE'].dt.day == d1]

def pick_except_year(df1,yr):
    df2 = df1[df1['CLEARTIMESTAMP'].dt.year != yr]
    return df2

def datediff(df1,newcolname,col1,col2=False):
    if col2 != False:
        df1 = conv_to_datetime(df1,col1)
        df1 = conv_to_datetime(df1,col2)
        df1 = pick_except_year(df1,1970)
        df2 = fdt.add_col_df(df1,newcolname)
        df2[newcolname] = df2[col2] - df2[col1]
        df2[newcolname] = df2[newcolname].astype('timedelta64[m]')
        return df2
    else:
        df1 = conv_to_datetime(df1,col1)
        df2 = fdt.add_col_df(df1,'now',datetime.now())
        df2 = conv_to_datetime(df2,'now')
        df3 = fdt.add_col_df(df2,newcolname)
        df3[newcolname] = df3['now'] - df3[col1]
        df3[newcolname] = df3[newcolname].astype('timedelta64[m]')
        df3.drop('now', axis='columns', inplace=True)
        return df3

#print(df0)
#print(df0.columns)



def cntif(df,*argv):
    rngmod = len(argv) % 2
    n = 0
    if rngmod == 0:
        ls = []
        while n<=len(argv):
            col = 'c' + str(n) + str(n+1)
            if isinstance(argv[n], pd.core.series.Series) and isinstance(argv[n+1], pd.core.series.Series):
                ls1 = argv[n].to_list()
                ls2 = argv[n+1].to_list()
                #ls = [i + j for i, j in zip(ls1, ls2)]
                print(ls2)
            elif isinstance(argv[n], pd.core.series.Series) and isinstance(argv[n+1], str):
                print('x')
            elif isinstance(argv[n+1], str) and isinstance(argv[n], pd.core.series.Series):
                print('x')
                n = n + 2
        print(ls)
    else:
        print('no of reference and no of criteria must be equal')


dfy = fst.catsemrw(df0)
dfz = get_region(dfy)
print('OK')
cntif(dfz,dfz['Region'],dfz['cat'])


def cifz(df,*argv):
    rng = len(argv) / 2
    if rng>=1:
        n = 0
        ls = []
        lss = []
        while n<=rng:
            if isinstance(argv[n], pd.core.series.Series) and isinstance(argv[n+1], pd.core.series.Series):
                ls1 =  argv[n].to_list()
                ls2 =  argv[n+1].to_list()
                if len(lss)<1:
                    lss = ls1
                    ls = ls2
                else:
                    l1 = []
                    l2 = []
                    l1 = [i + j for i, j in zip(lss, ls1)]
                    l2 = [i + j for i, j in zip(ls, ls2)]
                    lss = l1
                    ls = l2
            elif isinstance(argv[n], pd.core.series.Series) and isinstance(argv[n+1], str):
                ls1 =  argv[n].to_list()
                ar = np.repeat(argv[n+1], len(ls1))
                ls2 = ar.tolist()
                ls3 = [i + j for i, j in zip(ls1, ls2)]
                if len(lss)<1:
                    lss = ls1
                    ls = ls2
                else:
                    l1 = []
                    l2 = []
                    l1 = [i + j for i, j in zip(lss, ls1)]
                    l2 = [i + j for i, j in zip(ls, ls2)]
                    lss = l1
                    ls = l2
            n = n + 2
        df1 = pd.DataFrame(list(zip(lss, ls)),columns =['refrng','criteria'])
        dff = pd.concat([df, df1], axis=1, sort=False)
        print(dff)
        dfx = df1.groupby(['refrng','criteria']).count().to_frame(name = 'cnt').reset_index()
        #dd = df1.value_counts(['refrng','criteria'])).counts()
        print(dfx)
        #dfy = dff.merge(dfx, on='refrng')
        #df2.drop(st, axis='columns', inplace=True)
        #print(dfx)

#cifz(dfz,dfz['Region'],dfz['cat'])

#dfx = datediff(df0,'datedff','LASTOCCURRENCE')

#print(dfz)
#cat Region

#dfz['rgcat'] = dfz['Region'] + dfz['cat']
#d1 = dfz.groupby(['rgcat'])['rgcat'].count().to_frame(name = 'counts').reset_index()
#ls = ['Region','cat']
#dx = countifs(dfz,ls,'count')
#print(dx)
#print(dfz['concat'])


#df3 = df2.groupby(['CUSTOMATTR15','diff'])['CUSTOMATTR15'].count()
#print(df3)
#dfx = df2.groupby(['CUSTOMATTR15']).CUSTOMATTR15.count().to_frame(name = 'SMX').reset_index()
#df3 = df2.groupby('CUSTOMATTR15')['diff'].sum().to_frame(name = 'SMX').reset_index()
#df3['SMX'] = df3['SMX'].astype(int)
#df3['SMX'] = df3['SMX'].astype(str).astype(int)
#df3['SMX'] = df3['SMX'].astype('int64').dtypes
#df3["SMX"] = df3["SMX"].astype(int)
#df3 = df2.groupby(df2['CUSTOMATTR15']).diff.sum().to_frame(name = 'SMX').reset_index()
#print(df3#)



#ds = df3.dtypes
#print(ds)
#print(type(df1['LASTOCCURRENCE']))
#df1['LASTOCCURRENCE'] = pd.to_datetime(df1['LASTOCCURRENCE'],format="%d/%m/%y, %H:%M:%S", errors='raise')
#df1['LASTOCCURRENCE'] = df1.apply(lambda x : pd.to_datetime(x.LASTOCCURRENCE).strftime("%d-%m-%Y h:M"), axis = 1)














#print(pt)
#df0 = pd.read_csv(pt)
#cols = ['Serial','EquipmentKey','LastOccurrence','Summary','AssociatedCR','TTSequence','TTStatus','CustomAttr15','BCCH','AlertKey','CustomAttr3','ClearTimestamp']
#df1 = df0[cols]
#df2 = look.catsemrw(df1)
#print(df2.head(5))
#df3 = look.process_sem_raw(df2)
#df4 = look.code_corr(df3)
#print(df4['CustomAttr15'])
#df.astype({'col1': 'int32'}).dtypes


#df1 = look.add_col_df(df,'cnt')
#
#df2 = df0.value_counts(dropna=False)
#print(df2)
#print(df2.shape[0])
#ls = df2.values.tolist()
#print(df2)
#print(df2.shape[1])

#df4 = pd.DataFrame(df3, columns=['1','2'])
#print(df4)
#df3 = df.merge(df2,)
#df1 = look.timediff(df,'LASTOCCURRENCE','CLEARTIMESTAMP',"MTTR")
#print(df1)dropna=False
#print(df1)
#print(df)

#df0 = look.countif(df,'Summary','Summary',"CAT")
#print(df0)
