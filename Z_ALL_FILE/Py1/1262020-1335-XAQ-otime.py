from datetime import *
from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *
import pandas as pd
import os
import numpy as np



#x = relativedelta(datetime(2003, 10, 24, 10, 0), datetime.now()).__format__
#print(x)


def parse_date_fuzzy(string, first='day'):
    x = ""
    try:
        if first == 'day':
            x = parse(string, fuzzy=True, dayfirst=True)
        elif first == 'year':
            x = parse(string, fuzzy=True, yearfirst=True)
        else:
            x = parse(string, fuzzy=True)
        return x.strftime("%Y-%m-%d")
    except:
        return ""

print(parse_date_fuzzy("INC 7/12/20"))

def conv_to_datetime(df1,col):
    df1[col] = pd.to_datetime(df1[col], errors='coerce')
    return df1

def pick_by_day(df1,day):
    df2 = df1[df1['LASTOCCURRENCE'].dt.day == day]

def pick_except_year(df1,yr):
    df2 = df1[df1['CLEARTIMESTAMP'].dt.year != yr]
    return df2


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

def con_sec(sec):
    time = float(sec)
    day = time // (24 * 3600)
    time = time % (24 * 3600)
    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time
    return "%d:%d:%d" % (hour + 24*day, minutes, seconds)

def datediff(unit,datetime1,datetime2):
    d1 = ""
    d2 = ""
    try:
        if isinstance(datetime1, str):
            d1 = parse(datetime1)
        elif isinstance(datetime1, datetime):
            d1 = datetime1
        if isinstance(datetime2, str):
            d2 = parse(datetime2)
        elif isinstance(datetime2, datetime):
            d2 = datetime2
        if unit == 'n':
            return round(abs((d1 - d2)).total_seconds()/60,3)
        elif unit == 'h':
            return round(abs((d1 - d2)).total_seconds()/3600,3)
        elif unit == 's':
            return round(abs((d1 - d2)).total_seconds(),3)
        elif unit == '':
            x = con_sec(abs(d1 - d2).total_seconds())
            return x
    except:
        return "NA"
    
    
def datediff_ondf(df1, newcolname, col1, col2=False):
    try:
        if col2 != False:
            df1 = conv_to_datetime (df1, col1)
            df1 = conv_to_datetime (df1, col2)
            df1 = pick_except_year (df1, 1970)
            df2 = add_col_df (df1, newcolname)
            df2[newcolname] = df2[col2] - df2[col1]
            df2[newcolname] = df2[newcolname].astype ('timedelta64[m]')
            return df2
        else:
            df1 = conv_to_datetime (df1, col1)
            df2 = add_col_df (df1, 'now', datetime.now ())
            df2 = conv_to_datetime (df2, 'now')
            df3 = add_col_df (df2, newcolname)
            df3[newcolname] = df3['now'] - df3[col1]
            df3[newcolname] = df3[newcolname].astype ('timedelta64[m]')
            df3.drop ('now', axis='columns', inplace=True)
            return df3
    except:
        print ("format like: datediff(df1,newcolname,colname,colname=False), it must not pd.core.series.Series")

def formatchk(L1):
    if isinstance(L1, list):
        return L1
    elif isinstance(L1, pd.core.series.Series):
        ls = L1.to_list()
        return ls

def ddiff(DT1, DT2):
    #serialize and convert using dateutil.parser and datetime.strftime
    ls1 = formatchk(DT1)
    ls2 = formatchk(DT2)
    if len(ls1) == len(ls2):
        lss = []
        for i in range(len(ls1)):
            dt1 = parse(str(ls1[i]))
            dt2 = parse(str(ls2[i]))
            if '1970' not in ls2[i]:
                diff = abs(dt2 - dt1)
                lss.append(diff.total_seconds()/60)
            else:
                diff = (datetime.now() - dt1)
                diff = abs(dt2 - dt1)
                lss.append(diff.total_seconds()/60)
        else:
            return lss

#diffdate = lambda T1, T2 : (datetime.strptime(T2, "%d/%m/%Y %H:%M") - datetime.strptime(T1, "%d/%m/%Y %H:%M")).total_seconds()/60
#diff_from_now = lambda locc : (datetime.now() - datetime.strptime(locc, "%d/%m/%Y %H:%M")).total_seconds()/60





def help_otime():
    a = """#df = pd.read_csv(os.getcwd() + "\\sclick.csv")
#print(df.columns)
#df = df.assign(dur = 0)
#x = ddiff(df['FO'],df['LO'])
#print(x)
#print(df[['LO','CLR','Diff']])"""
    print(a)
