import pandas as pd
import numpy as np
import os
from datetime import *



def join_list(ls1, ls2, ls3):
    ToDf = pd.DataFrame(zip(ls1, ls2, ls3))
    return ToDf

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

def conv_lst_dic(lsKy,lsVal):
    try:
        dc = dict(zip(lsKy, lsVal))
        return dc
    except:
        print('err')

def map_df_dic(df0,dc,onkey_col,newcolname):
    df = add_col_df(df0,newcolname)
    df[newcolname] = df[onkey_col].map(dc)
    return df

def df_add_list_col(df,nc,nwlst):
    dfx = add_col_df(df,nc)
    dfx[nc] = np.nan
    dfx[nc] = np.array(nwlst)
    return dfx

def vlookup1(df,refdic,refcol,nwcol):
    df[nwcol] = df.reset_index()[refcol].map(refdic).values
    return df

def str_cut(df,lst,newcolname,lft,rht):
    df.replace(r'^\s*$', 'UNK', regex=True)
    ls = list(map (lambda x: str(x[lft:rht]) if (len(str(x)) >= 6) else "NF", lst))
    df[newcolname] = np.nan
    df[newcolname] = np.array(ls)
    return df

def filter_e_3col(df,c1,c1val,c2,c2val,c3,c3val):
    df0 = df.loc[(df[c1]==c1val) & (df[c2]==c2val) & (df[c3]==c3val)]
    return df0
def filter_e_2col(df,c1,c1val,c2,c2val):
    df0 = df.loc[(df[c1]==c1val) & (df[c2]==c2val)]
    return df0
def filter_e_1col(df,c1,c1val):
    df0 = df.loc[(df[c1]==c1val)]
    return df0

def filter_p_ncol(ndf,refdic,oncolumn,newcol):
    df = ndf.replace(r'^\s*$', np.nan, regex=True)
    for i in range(len(df)):
        fnd = 0
        val = df.loc[i,oncolumn]
        for ky,vl in refdic.items():
            if ky in val:
                fnd = 1
                df.loc[i,newcol] = vl
                break
        if fnd == 0:
            df.loc[i,newcol] = "other"
    return df


def filter_p(df,reflst,oncolumn):
    i = 0
    dfx = pd.DataFrame([])
    rw = 0
    for k in reflst:
        i = i + 1
        ndf = df[df[oncolumn].str.contains(k)]
        rw = ndf.shape[0]
        if rw >= 2:
            if i == 1:
                dfx = ndf
            else:
                dfy = pd.concat([dfx,ndf])
                dfx = dfy
                dfy = pd.DataFrame([])
    else:
        return dfx

def cond_apply_list(lst,whichfn, clr = []):
    if whichfn == 'codecut':
        ls = list(map (lambda x: str(x[0:5]) if (len(str(x)) >= 6) else "NF", lst))
        return ls
    elif whichfn == 'agact':
        ls = list(map (lambda x: ((datetime.now() - datetime.strptime(x, "%d/%m/%Y %H:%M")).total_seconds())/60, lst))
        return ls
    elif whichfn == 'agclr':
        ls = list(map (lambda x , y: ((datetime.strptime(x, "%d/%m/%Y %H:%M") - datetime.strptime(y, "%d/%m/%Y %H:%M")).total_seconds())/60 if ('1970' not in str(y)) else "0", clr,lst))
        return ls



def process_sem_raw(df1):
    #df1 = df[['SERIAL','EQUIPMENTKEY','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','CUSTOMATTR3','IDENTIFIER']]
    LL1 = df1['CustomAttr15']
    LL2 = df1['LastOccurrence']
    #LL3 = df1['ClearTimestamp']
    sc = cond_apply_list(LL1,'codecut')
    ag = cond_apply_list(LL2,'agact')
    #agclr = cond_apply_list(LL2,'agclr',LL3)
    ndf1 = df_add_list_col(df1,'scode',sc)
    ndf2 = df_add_list_col(ndf1,'aging_now',ag)
    #ndf3 = df_add_list_col(ndf2,'MTTR',ag)
    ndf4 = catsemrw(ndf2)
    return ndf4

def countifs(ndf, c1 , ref1, c2 = False, ref2 = False, c3 = False , ref3 = False):
    c = 1
    df = ndf.replace(r'^\s*$', np.nan, regex=True)
    if c2 != False:
        if c3 != False:
            df0 = df.loc[(df[c1]==ref1) & (df[c2]==ref2) & (df[c3]==ref3)]
        else:
            df0 = df.loc[(df[c1]==ref1) & (df[c2]==ref2)]
    else:
        df0 = df.loc[(df[c1]==ref1)]
    return df0.shape[0]

def rmv_duplicates(ndf, list_of_columns):
    df = ndf.replace(r'^\s*$', np.nan, regex=True)
    df.drop_duplicates(subset=list_of_columns)
    return df

def sorting(ndf,oncol):
    df = ndf.replace(r'^\s*$', np.nan, regex=True)
    df.sort_values(by=oncol, ascending=False)

def sumifs(df,refcol,numeric_col):
    df['agsum'] = df.groupby(refcol)[numeric_col].sum()
    return df
    #df['agsum'] = df.groupby('pet').treats.transform('sum')

def match(df,indx,typ):
    pass

#### Custom For SEM DATA Process ###
