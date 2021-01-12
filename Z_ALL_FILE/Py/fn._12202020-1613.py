import pandas as pd
import numpy as np
from dateutil.parser import *
from datetime import *
import time

def add_col_df(df, colname, colval=False, indx=False):
    if not indx:
        if not colval:
            ndf = df.assign (coln='NWC')
            ndf.rename (columns={'coln': colname}, inplace=True)
            return ndf
        else:
            ndf = df.assign (coln=colval)
            ndf.rename (columns={'coln': colname}, inplace=True)
            return ndf
    else:
        if colval == False:
            df.insert (indx, colname, 'NWC', allow_duplicates=False)
            return df
        else:
            df.insert (indx, colname, colval, allow_duplicates=False)
            return df

def conv_to_datetime(df1, col):
    df1[col] = pd.to_datetime (df1[col], errors='coerce')
    return df1

def pick_by_day(df1, day):
    df2 = df1[df1['LASTOCCURRENCE'].dt.day == d1]

def pick_except_year(df1, yr):
    df2 = df1[df1['CLEARTIMESTAMP'].dt.year != yr]
    return df2

def filtering(df, oncol, lst, byhow):
    if byhow == 'out':
        df1 = df[~df[oncol].isin (lst)]
        return df1
    else:
        df1 = df[df[oncol].isin (lst)]
        return df1

def countif(col_as_range,criteria):
    # col_as_range can be list or daraframe series
    if isinstance(col_as_range,list):
        count = col_as_range.count(criteria)
        return count
    elif isinstance(col_as_range, pd.core.series.Series):
        col_range_list = col_as_range.values.tolist()
        count = col_range_list.count(criteria)
        return count
    else:
        return "none"

def instr(main_str, search_str, start_position = False):
    if (start_position == False):
        x = main_str.find(search_str)
        return x
    else:
        ln = len(main_str) - start_position
        y = main_str[-ln:]
        x = y.find(search_str)
        return x

def instrrev(main_str, search_str, start_position = False):
    if (start_position == False):
        x = main_str.rfind(search_str)
        return x
    else:
        ln = len(main_str) - start_position
        y = main_str[-ln:]
        x = y.rfind(search_str)
        return x

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

def aplist(L1,L2):
    ls = []
    if isinstance(L1, pd.core.series.Series) and isinstance(L2, pd.core.series.Series):
        ls1 = L1.to_list()
        ls2 = L2.to_list()
        ls = [i + j for i, j in zip(ls1, ls2)]
    elif isinstance(L1, list) and isinstance(L2, list):
        ls = [i + j for i, j in zip(L1, L2)]
    elif isinstance(L1, pd.core.series.Series) and isinstance(L2, str):
        ls1 = L1.to_list()
        for i in range(len(ls1)):
            ni = str(ls1[i]) + L2
            ls.append(ni)
    elif isinstance(L1, list) and isinstance(L2, str):
        for i in range(len(ls1)):
            ni = str(ls1[i]) + L2
            ls.append(ni)
    else:
        print('arg1 can be list or pd.core.series.Series and arg2 can be string')
    return ls

def countifs(df0,newcolname,*argv):
    df = df0
    rngmod = len(argv) % 2
    n = 0
    m = 0
    ls = []
    stst = ""
    pds_cnt = 0
    st_cnt = 0
    cnt = -1
    if len(argv) > 0:
        while n<len(argv):
            if isinstance(argv[n], pd.core.series.Series):
                pds_cnt = pds_cnt + 1
            elif isinstance(argv[n], str):
                st_cnt = st_cnt + 1
            else:
                xx = 'incorrect datatype, datatype can be "str" or "pd.core.series.Series" only'
                return xx
            n = n + 1
        n = 0
        if st_cnt != 0:
            while n<len(argv):
                if isinstance(argv[n], pd.core.series.Series):
                    if len(ls) <= 1:
                        ls = argv[n].to_list()
                    else:
                        ls0 = argv[n].to_list()
                        ls1 = aplist(ls,ls0)
                        ls = ls1
                elif isinstance(argv[n], str):
                    if stst == "":
                        stst = argv[n]
                    else:
                        stst = stst + argv[n]
                n = n + 1
            try:
                cnt = ls.count(stst)
            except:
                cnt = 0
        else:
            while n<len(argv):
                if isinstance(argv[n], pd.core.series.Series):
                    if len(ls) <= 1:
                        ls = argv[n].to_list()
                    else:
                        ls0 = argv[n].to_list()
                        ls1 = aplist(ls,ls0)
                        ls = ls1
                n = n + 1
            df1 = add_col_df(df,newcolname)
            df1[newcolname] = pd.Series(ls)
            xyz = "cnt-" + str(df0.shape[1]) 
            df2 = df1.groupby([newcolname])[newcolname].count().to_frame(name = xyz).reset_index()
            df = df1.merge(df2, on=newcolname)
            df = df.drop(newcolname, axis='columns')
        if cnt == -1:
            return df
        else:
            return cnt

def match(srcstr,list_as_range,start_from = False):
    try:
        if start_from == False or start_from == "First":
            if isinstance(list_as_range,list):
                indices = [i for i, x in enumerate(list_as_range) if x == srcstr]
                return indices[0]
            elif isinstance(list_as_range, pd.core.series.Series):
                col_range_list = list_as_range.values.tolist()
                indices = [i for i, x in enumerate(col_range_list) if x == srcstr]
                return indices[0]
            else:
                return "none"
        elif start_from == "Last":
            if isinstance(list_as_range,list):
                indices = [i for i, x in enumerate(list_as_range) if x == srcstr]
                ln = len(indices)
                return indices[ln-1]
            elif isinstance(list_as_range, pd.core.series.Series):
                col_range_list = list_as_range.values.tolist()
                indices = [i for i, x in enumerate(col_range_list) if x == srcstr]
                ln = len(indices)
                return indices[ln-1]
            else:
                return "none"
    except:
        return "NA"

def vlookup(lookup_str_or_df, ref_df_or_dict, ref_match_col_name, ref_pic_pick_col_name):
    if isinstance(lookup_str_or_df, pd.DataFrame):
        print("here")
        if isinstance(ref_df_or_dict,dict):
            lookup_str_or_df[ref_pic_pick_col_name] = lookup_str_or_df.reset_index()[ref_match_col_name].map(ref_df_or_dict).values
            return lookup_str_or_df
        else:
            df = ref_df_or_dict[[ref_match_col_name,ref_pic_pick_col_name]]
            print(df)
            ndf = lookup_str_or_df.merge(df, on=ref_match_col_name)
            return ndf                   
    if isinstance(lookup_str_or_df, str):
        try:
            if isinstance(ref_df_or_dict,dict):
                lsky = list(ref_df_or_dict.keys())
                lsval = list(ref_df_or_dict.values())
                indx = [i for i, x in enumerate(lsky) if x == lookup_str_or_df]
                return lsval[indx[0]]
            elif isinstance(ref_df_or_dict,pd.DataFrame):
                list_as_range = ref_df_or_dict[ref_match_col_name].values.tolist()
                pick_list = ref_df_or_dict[ref_pic_pick_col_name].values.tolist()
                indx = [i for i, x in enumerate(list_as_range) if x == lookup_str_or_df]
                return pick_list[indx[0]]
        except:
            return "none"


#print(match('n',df['column_1'],"Last"))
#d1 = "2020-11-06 13:05"
#d2 = "10-02-2020 11:05"
#nw = datetime.now()
#print(datediff('',d1,nw))
#a = "DHSDR01WC"
#print(instr(a,"SDR"))
#print(instr(a,"werqw", 1))
#print(vlookup(df,my_dict,"scode","state"))