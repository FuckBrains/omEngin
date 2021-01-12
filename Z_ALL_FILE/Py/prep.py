
import pandas as pd
import numpy as np
import os
import lookup.lookup as look
import func.fnstr as fst
import func.fndatetime as fdt
import func.fnlook as flk
import db.db as sq
import func.fnfn as fn
from datetime import *




def top10(df0):
    pt = os.getcwd() + "\\T10.csv"
    dfx = pd.read_csv(pt)
    if 'CUSTOMATTR15' in df0.columns:
        df0 = df0.rename(columns={'CUSTOMATTR15':'CODE'})
    df3 = flk.vlookup(df0,dfx,'CODE','NA')
    df4 = fst.add_col_df(df3,'NEMTTE')
    df4['NWMTTR'] = df4.apply(lambda x : x.MTTR/60, axis = 1)
    df5 = df4.groupby(df4['CODE']).NWMTTR.sum()
    df6 = pd.DataFrame(df5,columns=['CODE','SMM'])
    df7 = flk.vlookup(df4,df6,'CODE','NA')
    df8 = flk.countif(df7,'CODE','CODE','CNT')
    print(df8)

def process_sem_data(df0):
    if 'CLEARTIMESTAMP' in df0.columns:
        df2 = fdt.datedif(df0,'MTTR','LASTOCCURRENCE','CLEARTIMESTAMP')
    else:
        df2 = fdt.datedif(df0,'AGING','LASTOCCURRENCE')
    df3 = fst.code_corr(df2)
    df4 = fst.catsemrw(df3)
    df5 = fst.add_col_df(df4,'ShortCode')
    df5['ShortCode'] = df5.apply(lambda x : x.CUSTOMATTR15[0:5], axis = 1)
    cols = "ShortCode,Region"
    dfdb = sq.omdb(cols)
    df6 = flk.vlookup(df5,dfdb,'ShortCode','NA')
    df7 = fn.conct(df6,'CUSTOMATTR15','cat','CODECAT')
    df8 = df7.drop_duplicates(subset='CODECAT', keep="first", inplace=False)
    return df8