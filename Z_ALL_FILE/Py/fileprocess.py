import pandas as pd
import numpy as np
import os
from datetime import *
pd.options.mode.chained_assignment = None  # default='warn'

pt = os.getcwd()
alarm = pt + "\\C.csv"

df0 = pd.read_csv(alarm)
df1 = df0[['SERIAL','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','CUSTOMATTR3']]



def df_add_list_col(dfx,nc,nwlst):
    dfx[nc] = np.nan
    dfx[nwcol] = np.array(nwlst)
    return dfx

def vlookup(df,refdic,refcol,nwcol):
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

def datedif(ndf,nwcol,dt_col1,dt_col2 = False):
    df = ndf.replace(r'^\s*$', np.nan, regex=True)
    if dt_col2 == False:
        lst = df[dt_col1]
        ls = list(map (lambda x: ((datetime.now() - datetime.strptime(x, "%d/%m/%Y %H:%M")).total_seconds())/60, lst))
    else:
        lst = df[dt_col1]
        clr = df[dt_col2]
        ls = list(map (lambda x , y: ((datetime.strptime(x, "%d/%m/%Y %H:%M") - datetime.strptime(y, "%d/%m/%Y %H:%M")).total_seconds())/60 if ('1970' not in str(y)) else "0", clr,lst))
    df[nwcol] = np.nan
    df[nwcol] = np.array(ls)
    print('In Minutes')
    return df
        
def process_sem_raw(df):
    df1 = df[['SERIAL','EQUIPMENTKEY','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','CUSTOMATTR3','IDENTIFIER']]
    LL1 = df1['CUSTOMATTR15']
    LL2 = df1['LASTOCCURRENCE']
    LL3 = df1['CLEARTIMESTAMP']
    sc = cond_apply_list(LL1,'codecut')
    ag = cond_apply_list(LL2,'agact')
    agclr = cond_apply_list(LL2,'agclr',LL3)
    ndf1 = df_add_list_col(df1,'scode',sc)
    ndf2 = df_add_list_col(ndf1,'aging_now',ag)
    ndf3 = df_add_list_col(ndf2,'MTTR',ag)
    print(ndf3)

def countifs(ndf, c1 , ref1, c2 = False, ref2 = False, c3 = False , Ref3 = False):
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

def rmv_duplicates(df, list_of_columns):
    df = ndf.replace(r'^\s*$', np.nan, regex=True)
    df.drop_duplicates(subset=list_of_columns)
    return df

def sorting(df,oncol):
    df = ndf.replace(r'^\s*$', np.nan, regex=True)
    df.sort_values(by=oncol, ascending=False)

def sumifs():
    pass
def match():
    pass

df1 = df0[['SERIAL','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','CUSTOMATTR3','IDENTIFIER']]
#xxx = str_cut(df1,df1['CUSTOMATTR15'],'shortcode',0,5)
lx = ['2G SITE','3G SITE']
dc = {'2G SITE':'2G','3G SITE':'3G'}
dc2 = {'HUW-2G SITE DOWN':"HW",'ERI-3G SITE DOWN':'ERI'}
#aq = filter_p(df1,lx,'SUMMARY')
#print(aq['SUMMARY'])
#aw = filter_p_ncol(df1,dc,'SUMMARY','cat')
#print(aw)
aqq = vlookup(df1,dc2,'SUMMARY','VLOOKUP')
print(aqq)
#print(aqq.loc[(aqq['VLOOKUP']=='ERI')])
#print(aqq.columns)
#x = df_add_col(df1,'scode','codecut')
#print(x)
#y = filter_e_2col(aqq,'SUMMARY','ERI-2G SITE DOWN','VLOOKUP','ERI',)
#x = countifs(aqq,'SUMMARY','ERI-3G SITE DOWN','VLOOKUP','ERI')
#print(y)
lst = ['SUMMARY','VLOOKUP']
za = aqq.drop_duplicates(subset=lst)
print(za)

asq = datedif(df1,'AG','LASTOCCURRENCE')
print(asq)



