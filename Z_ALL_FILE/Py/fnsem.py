import pandas as pd
import numpy as np
from datetime import *
import os
import fnfn as fn

pt = os.getcwd()

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

def catsemrw(df0):
    df = add_col_df(df0,'cat')
    df['cat'] = df.apply(lambda row: TS(row.SUMMARY), axis = 1)
    return df

def get_region(df,column_contains_code):
    codemap = ""
    if "\func" in pt:
        codemap = os.getcwd() + "\\scode_map.csv"
    else:
        codemap = os.getcwd() + "\\func\\scode_map.csv"
    dmap = pd.read_csv(codemap)
    df1 = add_col_df(df,'scode')
    df1['scode'] = df1[column_contains_code].apply(lambda x : x[0:5])
    df3 = vlookup(df1,dmap,'scode','NA')
    df3 = df3.drop('scode', axis='columns')
    return df3

def cat_region(df):
    df1 = catsemrw(df)
    df2 = get_region(df1)
    print(df2)

def code_corr(df):
    ndf = df
    for i in range(len(ndf)):
        Eky = str(ndf.loc[i,'EQUIPMENTKEY'])
        A15 = str(ndf.loc[i,'CUSTOMATTR15'])
        if A15 == 'UNKNOWN' and len(Eky) < 15:
            if len(Eky) == 7:
                df.loc[i,'CUSTOMATTR15'] = Eky
            elif len(Eky) == 10:
                df.loc[i,'CUSTOMATTR15'] = Eky[0:7]
            elif '_' in str(Eky):
                fnd =  Eky.find('_')
                if fnd > 7:
                    df.loc[i,'CUSTOMATTR15'] = Eky[0:7]
                else:
                    try:
                        df.loc[i,'CUSTOMATTR15'] = Eky[fnd:7]
                    except:
                        df.loc[i,'CUSTOMATTR15'] = "UNKNOWN"
    return df


def techwise(df0):
    G2 = ""
    G3 = ""
    G4 = ""
    df = df0.applymap(str)
    print(len(df))
    colcode = fn.match(df,'CUSTOMATTR15')
    colLo = fn.match(df,'LASTOCCURRENCE')
    colcat = fn.match(df,'cat')
    for i in range(len(df)):
        try:
            ct = df.iloc[i,colcat]
            if df.iloc[i,colcat] == "2G":
                if G2 == "":
                    ls = df.iloc[:,colcat].to_list()
                    cnt2 = ls.count("2G")
                    G2 = "2G: " + str(cnt2) + chr(10) + df.iloc[i,colcode] + ", " + df.iloc[i,colLo]
                else:
                    G2 = G2 + chr(10) + df.iloc[i,colcode] + ", " + df.iloc[i,colLo]
            elif df.iloc[i,colcat] == "3G":
                if G3 == "":
                    ls = df.iloc[:,colcat].to_list()
                    cnt3 = ls.count("3G")
                    G3 = "3G: " + str(cnt3) + chr(10) + df.iloc[i,colcode] + ", " + df.iloc[i,colLo]
                else:
                    G3 = G3 + chr(10) + df.iloc[i,colcode] + ", " + df.iloc[i,colLo]
            elif df.iloc[i,colcat] == "4G":
                if G4 == "":
                    ls = df.iloc[:,colcat].to_list()
                    cnt4 = ls.count("4G")
                    G4 = "4G: " + str(cnt4) + chr(10) + df.iloc[i,colcode] + ", " + df.iloc[i,colLo]
                else:
                    G4 = G4 + chr(10) + df.iloc[i,colcode] + ", " + df.iloc[i,colLo]
        except:
            print(i)
    G = G2 + chr(10) + chr(10) + G3 + chr(10) + chr(10) + G4
    print(G)
