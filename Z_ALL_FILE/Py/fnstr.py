import pandas as pd
import numpy as np

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

TS = lambda x : '2G' if ('2G SITE DOWN' in x) \
                else ('3G' if ('3G SITE DOWN' in x) \
                else ('4G' if ('4G SITE DOWN' in x) \
                else ('MF' if ('MAIN' in x) \
                else ('DC' if ('VOLTAGE' in x) \
                else ('TM' if ('TEMPERATURE' in x) \
                else ('SM' if ('SMOKE' in x) \
                else ('GN' if ('GEN' in x) \
                else ('GN' if ('GENSET' in x) \
                else ('TH' if ('THEFT' in x) \
                else ('C2G' if ('2G CELL DOWN' in x) \
                else ('C3G' if ('3G CELL DOWN' in x) \
                else ('C4G' if ('4G CELL DOWN' in x) \
                else "NA"))))))))))))

def Lcut(mstr,cut_to):
    try:
        if len(mstr) >= cut_to:
            x = mstr[0:cut_to]
            return x
        else:
            print("length of string is less than 'cut_to'")
    except:
        return mstr

def Rcut(mstr,cut_to):
    try:
        if len(mstr) - cut_to >= 0:
            a = len(mstr) - cut_to
            b = len(mstr)
            x = mstr[a:b]
            return x
        else:
            print("length of string is less than 'cut_to'")
    except:
        return mstr

def src_in_str(mstr,lkstr):
    if lkstr in mstr:
        return mstr.find(lkstr)
    else:
        return 0

def code_corr(df0):
    df = df0
    for i in range(len(df)):
       Eky = df.loc[i,'EQUIPMENTKEY']
       A15 = df.loc[i,'CUSTOMATTR15']
       if A15 == 'UNKNOWN' and Eky != 'UNKNOWN' and len(Eky)<15:
           if len(Eky) == 7:
               df.loc[i,'CUSTOMATTR15'] = Eky
           elif '_' in Eky:
               x = Eky.find('_')
               if x > 4:
                   df.loc[i,'CUSTOMATTR15'] = Lcut(Eky,7)
               else:
                   df.loc[i,'CUSTOMATTR15'] = Rcut(Eky,7)
    return df

def catsemrw(df0):
    df = add_col_df(df0,'cat')
    df['cat'] = df.apply(lambda row: TS(row.SUMMARY), axis = 1)
    return df

def get_region(df):
    df4 = df
    df5 = flk.add_col_df(df4,'ShortCode')
    df5['ShortCode'] = df5.apply(lambda x : x.CUSTOMATTR15[0:5], axis = 1)
    cols = "ShortCode,Region"
    dfdb = sq.omdb(cols)
    df6 = flk.vlookup(df5,dfdb,'ShortCode','NA')
    df6.drop('ShortCode', axis='columns', inplace=True)
    return df6
