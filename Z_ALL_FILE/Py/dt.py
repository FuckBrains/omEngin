import pandas as pd
#from datetime import *
from dateutil.relativedelta import *
import numpy as np
from oFn.fn import *
import oFn.fnfn as fnx

def PP(df):
    try:
        print(df['LASTOCCURRENCE', 'DUR', 'DURCAT'])
    except:
        try:
            print(df['LASTOCCURRENCE', 'DUR'])
        except:
            print(df['LASTOCCURRENCE'])
            
def series2df(sr1, sr2):
    df = pd.concat([sr1, sr2], axis=1)
    return df

def fmtconv(ls):
    df['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].apply(lambda x : pd.to_datetime(x, errors='coerce', dayfirst = True, cache=True).strftime("%Y/%m/%d %H:%M:%S"))

def DateDiff(df, newcol, col1, col2 = False, DayFirst = True):
    if col2 == False:
        lscol = df[col1].to_list()
        try:
            ls = list(map (lambda x: ((datetime.now() - datetime.strptime(x, "%d/%m/%Y %H:%M")).total_seconds())/60, lscol))
        except:
            df1 = fnx.add_col_df(df, 'newcol')
            df1[newcol] = np.array(ls)
    else:
        lscol1 = df[col1].to_list()
        lscol2 = df[col2].to_list()
        ls = list(map (lambda x , y: ((datetime.strptime(x, "%d/%m/%Y %H:%M") - datetime.strptime(y, "%d/%m/%Y %H:%M")).total_seconds())/60 if ('1970' not in str(y)) else "0", lscol2,lscol1))
        df1 = fnx.add_col_df(df, 'newcol')
        df1[newcol] = np.array(ls)
    df[newcol] = df[newcol].astype(float).round(2)
    return df
    
def xxz(df):
    df['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].apply(lambda x : pd.Timestamp(x))
    return df

def Sr2Tstamp(df):
    df['LASTOCCURRENCE'] = df['LASTOCCURRENCE'].to_timestamp
    return df

def DateTime(df, nwcol, col1, col2 = False):
    df[col1] = df[col1].apply(lambda x : pd.to_datetime(x, errors='coerce', yearfirst = True, cache=True).strftime("%Y/%m/%d %H:%M:%S"))
    dfx = df.convert_dtypes ()
    dfx.assign(nwcol = 0)
    if col2 == False:
        n = datetime.now ()
        xx = n.strftime("%Y/%m/%d %H:%M:%S")
        dfx.assign(TEMPCOL= xx)
        fnx.datediff()
        try:
            dfx[nwcol] = dfx.apply(lambda x : n.strftime("%Y/%m/%d %H:%M:%S") - x[col1], axis = 1)
        except:
            
            dfx[nwcol] = dfx['NW'] - dfx[col1]
    else:
        print('x')

    

#pt = os.getcwd() + "\\"
#df = pd.read_csv(pt + 'P.csv')
#xd = DateTime(df)
#Delta(xd)
#Sr2Tstamp(df)
#xxz(df)
#print(xa)