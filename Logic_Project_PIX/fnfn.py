import pandas as pd
import numpy as np
from datetime import *

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


def conct(df, col1, col2, newcolname, seperator=False):
    if seperator == False:
        try:
            df1 = add_col_df (df, newcolname)
            df1[newcolname] = df1[col1].str.cat (df1[col2])
            return df1
        except:
            print ('conct: column name not found in dataframe or dataframe is not valid dataframe')
    else:
        try:
            df1 = add_col_df (df, newcolname)
            df1[newcolname] = df1[col1].str.cat (df1[col2], sep=seperator)
            return df1
        except:
            print ('conct: column name not found in dataframe or dataframe is not valid dataframe')


def conv_lst_dic(lsKy, lsVal):
    try:
        dc = dict (zip (lsKy, lsVal))
        return dc
    except:
        print ('err')


def map_df_dic(df0, dc, onkey_col, newcolname):
    df = add_col_df (df0, newcolname)
    df[newcolname] = df[onkey_col].map (dc)
    return df

def df_add_list_col(df, nc, nwlst):
    dfx = add_col_df (df, nc)
    dfx[nc] = np.nan
    dfx[nc] = np.array (nwlst)
    return dfx


def rmv_duplicates(ndf, list_of_columns):
    df = ndf.replace (r'^\s*$', np.nan, regex=True)
    df.drop_duplicates (subset=list_of_columns)
    return df


def sort_dsc(ndf, oncol):
    df = ndf.replace (r'^\s*$', np.nan, regex=True)
    df.sort_values (by=oncol, ascending=False)


def sort_asc(ndf, oncol):
    df = ndf.replace (r'^\s*$', np.nan, regex=True)
    df = df.sort_values (by=oncol, ascending=True)
    return df


def left(df, sdf, i):
    df1 = add_col_df (df, 'left_apply')
    df1['left_apply'] = sdf.apply (lambda x: x[0:i])
    return df1


def right(df, sdf, i):
    df1 = add_col_df (df, 'right_apply')
    df1['left_apply'] = sdf.apply (lambda x: x[-i:len (x)])
    return df1


def vlookup(df0, refdic, refcol, nwcol):
    if isinstance (refdic, dict):
        try:
            df = add_col_df (df0, nwcol)
            df[nwcol] = df.reset_index ()[refcol].map (refdic).values
            return df
        except:
            df = map_df_dic (df0, refdic, refcol, nwcol)
            return df
    else:
        ndf = df0.merge (refdic, on=refcol)
        return ndf


def countifz(df, list_of_cols_as_ref, newcol):
    if len (list_of_cols_as_ref) > 1:
        st = ""
        for i in range (len (list_of_cols_as_ref)):
            if st == '':
                st = list_of_cols_as_ref[i]
            else:
                st = st + '-' + list_of_cols_as_ref[i]
        df[st] = df[list_of_cols_as_ref].apply (lambda x: ''.join (map (str, x)), axis=1)
        df1 = df.groupby (st)[st].count ().to_frame (name=newcol).reset_index ()
        df2 = df.merge (df1, on=st)
        df2.drop (st, axis='columns', inplace=True)
        return df2
    else:
        col = list_of_cols_as_ref[0]
        df1 = df.groupby (col)[col].count ().to_frame (name=newcol).reset_index ()
        df2 = df.merge (df1, on=col)
        return df2


def datediff(df1, newcolname, col1, col2=False):
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


def aplist(L1, L2):
    ls = []
    if isinstance (L1, pd.core.series.Series) and isinstance (L2, pd.core.series.Series):
        ls1 = L1.to_list ()
        ls2 = L2.to_list ()
        ls = [i + j for i, j in zip (ls1, ls2)]
    elif isinstance (L1, list) and isinstance (L2, list):
        ls = [i + j for i, j in zip (L1, L2)]
    elif isinstance (L1, pd.core.series.Series) and isinstance (L2, str):
        ls1 = L1.to_list ()
        for i in range (len (ls1)):
            ni = str (ls1[i]) + L2
            ls.append (ni)
    elif isinstance (L1, list) and isinstance (L2, str):
        for i in range (len (L1)):
            ni = str (L1[i]) + L2
            ls.append (ni)
    else:
        print ('arg1 can be list or pd.core.series.Series and arg2 can be string')
    return ls


def countifs(df0, *argv):
    df = df0
    rngmod = len (argv) % 2
    n = 0
    m = 0
    ls = []
    stst = ""
    pds_cnt = 0
    st_cnt = 0
    cnt = -1
    if len (argv) > 0:
        while n < len (argv):
            if isinstance (argv[n], pd.core.series.Series):
                pds_cnt = pds_cnt + 1
            elif isinstance (argv[n], str):
                st_cnt = st_cnt + 1
            else:
                xx = 'incorrect datatype, datatype can be "str" or "pd.core.series.Series" only'
                return xx
            n = n + 1
        print (pds_cnt, st_cnt)
        n = 0
        if st_cnt != 0:
            while n < len (argv):
                if isinstance (argv[n], pd.core.series.Series):
                    if len (ls) <= 1:
                        ls = argv[n].to_list ()
                    else:
                        ls0 = argv[n].to_list ()
                        ls1 = aplist (ls, ls0)
                        ls = ls1
                elif isinstance (argv[n], str):
                    if stst == "":
                        stst = argv[n]
                    else:
                        stst = stst + argv[n]
                n = n + 1
            try:
                cnt = ls.count (stst)
            except:
                cnt = 0
        else:
            while n < len (argv):
                if isinstance (argv[n], pd.core.series.Series):
                    if len (ls) <= 1:
                        ls = argv[n].to_list ()
                    else:
                        ls0 = argv[n].to_list ()
                        ls1 = aplist (ls, ls0)
                        ls = ls1
                n = n + 1
            df1 = add_col_df (df, 'NC1')
            df1['NC1'] = pd.Series (ls)
            df2 = df1.groupby (['NC1']).NC1.count ().to_frame (name='cnt').reset_index ()
            df = df1.merge (df2, on='NC1')
            df = df.drop ('NC1', axis='columns')
        print (cnt)
        if cnt == -1:
            return df
        else:
            return cnt


def match(df, *argv):
    x = 0
    n = 0
    st_cnt = 0
    pds_cnt = 0
    if len (argv) > 0 and len (argv) <= 3:
        while n < len (argv):
            if isinstance (argv[n], pd.core.series.Series):
                pds_cnt = pds_cnt + 1
            elif isinstance (argv[n], str) or isinstance (argv[n], int):
                st_cnt = st_cnt + 1
            else:
                xx = 'incorrect datatype, datatype can be "str" or "int" or "pd.core.series.Series" only'
                return xx
            n = n + 1
        if pds_cnt == 0:
            colList = df.columns.to_list ()
            for i in range (len (colList)):
                if colList[i] == argv[0]:
                    x = i
                    break
            return x
        else:
            try:
                manner = argv[2]
            except:
                manner = 'none'
            if isinstance (argv[0], pd.core.series.Series):
                if manner == 'none' or manner == 'first':
                    idx = df[argv[0] == argv[1]].index[0]
                    return idx
                else:
                    idx = df[argv[0] == argv[1]].index
                    ln = len (idx)
                    if manner == 'last':
                        return idx[ln - 1]
                    elif manner == 'all':
                        return idx
                    else:
                        err = "command can be 'first' or 'last' or 'all'"
                        return err
    else:
        xx = "Match works only for a string or int on single col/series element"
        return xx


def sumifz(df, list_of_cols_as_ref, numeric_col, newcol):
    if len (list_of_cols_as_ref) > 1:
        st = ""
        for i in range (len (list_of_cols_as_ref)):
            if st == '':
                st = list_of_cols_as_ref[i]
            else:
                st = st + '-' + list_of_cols_as_ref[i]
        df[st] = df[list_of_cols_as_ref].apply (lambda x: ''.join (map (str, x)), axis=1)
        df1 = df.groupby (st)[numeric_col].sum ().to_frame (name=newcol).reset_index ()
        df2 = df.merge (df1, on=st)
        df2.drop (st, axis='columns', inplace=True)
        return df2
    else:
        col = list_of_cols_as_ref[0]
        df1 = df.groupby (col)[numeric_col].sum ().to_frame (name=newcol).reset_index ()
        df2 = df.merge (df1, on=col)
        return df2


def instr(strtext, srcvalue, start_pos=False):
    if start_pos:
        st = strtext[start_pos:]
        f = st.find (srcvalue)
        return f
    else:
        f = strtext.find (srcvalue)
        return f

def instrrev(strtext, srcvalue, start_pos=False):
    if start_pos:
        st = strtext[start_pos:]
        f = st.rfind (srcvalue)
        return f
    else:
        f = strtext.rfind (srcvalue)
        return f


def sumifs(df0, numeric_col, *argv):
    df = df0
    rngmod = len (argv) % 2
    n = 0
    m = 0
    ls = []
    stst = ""
    pds_cnt = 0
    st_cnt = 0
    cnt = -1
    if len (argv) > 0:
        while n < len (argv):
            if isinstance (argv[n], pd.core.series.Series):
                pds_cnt = pds_cnt + 1
            elif isinstance (argv[n], str):
                st_cnt = st_cnt + 1
            else:
                xx = 'incorrect datatype, datatype can be "str" or "pd.core.series.Series" only'
                return xx
            n = n + 1
        print (pds_cnt, st_cnt)
        n = 0
        if st_cnt != 0:
            while n < len (argv):
                if isinstance (argv[n], pd.core.series.Series):
                    if len (ls) <= 1:
                        ls = argv[n].to_list ()
                    else:
                        ls0 = argv[n].to_list ()
                        ls1 = aplist (ls, ls0)
                        ls = ls1
                elif isinstance (argv[n], str):
                    if stst == "":
                        stst = argv[n]
                    else:
                        stst = stst + argv[n]
                n = n + 1
            try:
                cnt = ls.count (stst)
            except:
                cnt = 0
        else:
            while n < len (argv):
                if isinstance (argv[n], pd.core.series.Series):
                    if len (ls) <= 1:
                        ls = argv[n].to_list ()
                    else:
                        ls0 = argv[n].to_list ()
                        ls1 = aplist (ls, ls0)
                        ls = ls1
                n = n + 1
            print (ls)
            df1 = add_col_df (df, 'NC1')
            df1['NC1'] = pd.Series (ls)
            print (df1)
            df2 = df1.groupby (['NC1'])[numeric_col].sum ().to_frame (name='sumifs').reset_index ()
            print (df2)
            df = df1.merge (df2, on='NC1')
            df = df.drop ('NC1', axis='columns')
        print (cnt)
        if cnt == -1:
            return df
        else:
            return cnt
