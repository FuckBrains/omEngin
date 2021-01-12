import pandas as pd
from datetime import *
import os

def sec_to_dur(sec):
    time = float(sec)
    day = time // (24 * 3600)
    time = time % (24 * 3600)
    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time
    return "%d:%d:%d" % (hour + 24*day, minutes, seconds)

def dfdiff(df, LO, CLR = False):
    df = df.astype (str)
    if CLR == False:
        df[LO] = df.apply (lambda x: pd.to_datetime (x[LO]), axis=1)
        df = df.assign (DUR=df.apply (lambda x: pd.Timedelta (datetime.now() - x[LO]).seconds / 60, axis=1))
        return df
    else:
        df[LO] = df.apply (lambda x: pd.to_datetime (x[LO]), axis=1)
        df[CLR] = df.apply (lambda x: pd.to_datetime (x[CLR]), axis=1)
        df = df.assign(DUR=df.apply (lambda x: pd.Timedelta (x[LO] - x[CLR]).seconds / 60 if (
                x[CLR].year >= 2019) else "ACT", axis=1))
        return df

def series2df(sr1, sr2):
    df = pd.concat([sr1, sr2], axis=1)
    return df

def datetime_convert_format(df, col, fmt="%Y/%m/%d %H:%M:%S"):
    try:
        df[col] = df[col].apply(lambda x : pd.to_datetime(x, errors='coerce', dayfirst = True, cache=True).strftime(fmt))
        return df
    except:
        df[col] = df[col].apply(lambda x: pd.to_datetime (x, errors='coerce', yearfirst=True, cache=True).strftime(fmt))
        return df

def vL(df_Main, df_Ref, col='Code', pick_from_ref = ['Zone']):
    ls = df_Main.columns.to_list ()
    df1 = df_Main.merge (df_Ref, how='right', on=col)
    for i in pick_from_ref:
        ls.append(str(i))
    else:
        dfx = df1[ls]
        return dfx

def ofn():
    print("func")
    print("dfdiff(df, LO, CLR = False) #if CRL=False, calcute from now, return minutes")
    print("sec_to_dur(sec), convert second into hh:mm:ss")
    print("""datetime_convert_format(df, col, fmt="%Y/%m/%d %H:%M:%S")""")
    print("vL(df, db, col='Code', pick_from_ref=['Zone','Cluster'])")
    
print("moduel 'oTime': on datetime, call ofn() to see function")
#vlook = vL(df, db, col='Code', pick_from_ref=['Zone','Cluster'])
#df = pd.read_csv(os.getcwd() + "\\csv\\TIME_TEST.csv", low_memory=False)
#df = df.astype(str)
#print(df.columns)