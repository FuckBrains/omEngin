import pandas as pd
import numpy as np
from datetime import *


nw = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

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

def timediff(df,c1,c2,newcol):
    df[c1] = pd.to_datetime(df[c1])
    df[c2] = pd.to_datetime(df[c2])
    df1 = add_col_df(df,newcol)
    df1[newcol] = abs(df1[c2] - df1[c1])
    df1[newcol] = df1[newcol].astype("i8")/1e9
    df1[newcol] = df1[newcol] / 60
    return df1

def timediff_2(df,c1,c2,newcol):
    df[c1] = pd.to_datetime(df[c1])
    df[c2] = pd.to_datetime(df[c2])
    df1 = add_col_df(df,newcol)
    df1[newcol] = abs(df1[c2] - df1[c1])
    df1[newcol] = df1[newcol].astype('timedelta64[m]')
    return df1

def datedif(ndf,nwcol,dt_col1,dt_col2 = False):
    df = ndf.replace(r'^\s*$', np.nan, regex=True)
    if dt_col2 == False:
        df1 = add_col_df(df,'NOW',nw)
        df2 = timediff(df1,dt_col1,'NOW',nwcol)
        df3 = df2.drop(['NOW'], axis = 1)
        return df3
        #lst = df[dt_col1]
        #ls = list(map (lambda x: ((datetime.now() - datetime.strptime(x, "%d/%m/%Y %H:%M")).total_seconds())/60, lst))
    else:
        df2 = timediff(df,dt_col1,dt_col2,nwcol)
        return df2
        #ls = list(map (lambda x , y: ((datetime.strptime(x, "%d/%m/%Y %H:%M") - datetime.strptime(y, "%d/%m/%Y %H:%M")).total_seconds())/60 if ('1970' not in str(y)) else "0", clr,lst))
    #df[nwcol] = np.nan
    #df[nwcol] = np.array(ls)
    #print('In Minutes')


def datediff(unit,Time1,Time2):
    print(type(Time1))
    print(type(Time2))
