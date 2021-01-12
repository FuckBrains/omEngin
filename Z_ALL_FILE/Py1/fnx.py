import pandas as pd
import numpy as np
from datetime import *


df0 = pd.DataFrame([['Iphone','DHDEM26','30-10-2020 12:14','25-11-2020 12:24','400'],['Iphone','CGHTZ09','11-09-2020 12:14','22-11-2020 12:24','400'],
['dell','LXRGN32','11-09-2020 12:14','19-11-2020 12:24','300'],['dell','LXRGN31', '11-09-2020 12:13','11-20-2020 12:24','300'],
['Samsung ','SGSJP04', '11-09-2020 12:12','11-20-2020 12:24','250'],['Samsung ','CXMHK36', '11-09-2020 12:11','11-20-2020 12:24','250'],
['Samsung ','CGFTK29', '11-09-2020 12:10','11-20-2020 12:24','250'],['dell','CGKTLB6','11-09-2020 12:10','11-20-2020 12:24','300'],
['dell','null', '11-09-2020 12:10','11-20-2020 12:24','300']],columns=('PRODUCT','ZIPCODE','SHIPMENT','DELIVERY','PRICE'))

def aplist(L1,L2):
    ls = []
    if isinstance(L1, pd.core.series.Series) and isinstance(L2, pd.core.series.Series):
        ls1 = L1.to_list()
        ls2 = L2.to_list()
        ls = [i + j for i, j in zip(ls1, ls2)]
    elif isinstance(L1, list) and isinstance(L2, list):
        ls = [i + j for i, j in zip(L1, L2)]
    elif isinstance(L1, pd.core.series.Series) and isinstance(L2, str):
        ls1 = L1.to_list()
        for i in range(len(ls1)):
            ni = str(ls1[i]) + L2
            ls.append(ni)
    elif isinstance(L1, list) and isinstance(L2, str):
        for i in range(len(ls1)):
            ni = str(ls1[i]) + L2
            ls.append(ni)
    else:
        print('arg1 can be list or pd.core.series.Series and arg2 can be string')
    return ls

def sumifs(df,numeric_col,list_of_cols_as_ref):
    if len(list_of_cols_as_ref) > 1:
        st = ""
        for i in range(len(list_of_cols_as_ref)):
            if st == '':
                st = list_of_cols_as_ref[i]
            else:
                st = st + '-' + list_of_cols_as_ref[i]
        df[st] = df[list_of_cols_as_ref].apply(lambda x: ''.join(map(str,x)),axis=1)
        df1 = df.groupby(st)[numeric_col].sum().to_frame(name = newcol).reset_index()
        df2 = df.merge(df1, on=st)
        df2.drop(st, axis='columns', inplace=True)
        return df2
    else:
        col = list_of_cols_as_ref[0]
        df1 = df.groupby(col)[numeric_col].sum().to_frame(name = newcol).reset_index()
        df2 = df.merge(df1, on=col)
        return df2


def cntff(df, numeric_col, *argv):
    rngmod = len(argv) % 2
    if len(argv) > 0 and rngmod == 0:
        n = 0
        lscnt = 0
        stcnt = 0
        lsTmp = []
        ls = []
        st = ""
        while n < len(argv):
            if isinstance(argv[n], pd.core.series.Series):
                if len(ls) < 1:
                    ls = argv[n]
                else:
                    lsTmp = aplist(ls,argv[n])
                    ls = lsTmp
                lscnt = lscnt + 1
            else:
                stcnt = stcnt + 1
                st = st + str(argv[n])
            n = n + 1
        df['NC1'] = pd.Series(ls)
        if stcnt == lscnt:
            df1 = df.groupby(st)[numeric_col].sum().to_frame(name = "X").reset_index()
        elif stcnt == 0:
            df1 = df.groupby([ls])[numeric_col].sum().to_frame(name = "X").reset_index()
        print(df1)
