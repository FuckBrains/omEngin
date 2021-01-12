import pandas as pd
import numpy as np
import os
from datetime import *


def add_col_df(df, colname, colval = False, indx=False):
    if indx == False:
        if colval == False:
            ndf = df.assign(coln = 'NWC')
            ndf.rename(columns = {'coln': colname}, inplace = True)
            return ndf
        else:
            ndf = df.assign(coln = colval)
            ndf.rename(columns = {'coln': colname}, inplace = True)
            return ndf
    else:
        if colval == False:
            df.insert(indx, colname, 'NWC', allow_duplicates=False)
            return df
        else:
            df.insert(indx, colname, colval, allow_duplicates=False)
            return df

def map_df_dic(df0,dc,onkey_col,newcolname):
    df = add_col_df(df0,newcolname)
    df[newcolname] = df[onkey_col].map(dc)
    return df

def conv_lst_dic(lsKy,lsVal):
    try:
        dc = dict(zip(lsKy, lsVal))
        return dc
    except:
        print('err')

def vlookup(df0,refdic,refcol,nwcol):
    if isinstance(refdic,dict):
        try:
            df = add_col_df(df0, nwcol)
            df[nwcol] = df.reset_index()[refcol].map(refdic).values
            return df
        except:
            df = map_df_dic(df0,refdic,refcol,nwcol)
            return df
    else:
        ndf = df0.merge(refdic, on=refcol)
        return ndf

def sumif(df2,refcol,numeric_col,newcol):
    df3 = df2.groupby(refcol)[numeric_col].sum().to_frame(name = newcol).reset_index()
    dic = conv_lst_dic(df3[refcol],df3[newcol])
    df4 = map_df_dic(df2,dic,refcol,'sumif')
    return df4

def countif(df0,refcolumn,datacol,newcolname = False):
    if isinstance(refcolumn,str):
        df = add_col_df(df0, newcolname)
        rdf = df[refcolumn]
        reflst = rdf.values.tolist()
        vdf = df[datacol]
        nwlst = []
        for i in vdf:
            try:
                count = reflst.count(i)
                nwlst.append(count)
            except:
                nwlst.append('0')
    df[newcolname] = nwlst
    return df

df = pd.read_csv("C:\\Users\\kabir.omi\\Desktop\\B2.csv")
print(df.columns)
#df["N1"] = df.apply(lambda x : x.Cat + x.CustomAttr11, axis=1) #using 
#x = type(df)
##print(type(df['Cat']))
#print(type(df))
#print(df['Cat'])
print(df['Cat'].to_list().count('2G'))
print(df['Cat'].value_counts())
