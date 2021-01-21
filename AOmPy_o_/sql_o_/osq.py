import pandas as pd
import os
from tbl_mssql import *

def attempt_dt(df):
    ls = df.columns.to_list()
    for i in range(len(ls)):
        cname = ls[i]
        try:
            df[cname] = df.apply(lambda x : pd.to_datetime(x[cname]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
        except:
            pass
    return df

def drop_cols(df, col2drop=[]):
    if len (col2drop) > 0:
        cols = df.columns.to_list ()
        ncols = []
        for i in range (len (cols)):
            match = 0
            for j in range (len (col2drop)):
                if cols[i] == col2drop[j]:
                    match = 1
            if match == 0:
                ncols.append (cols[i])
        ndf = df[ncols]
        return ndf
    else:
        return df


def forupdate(df, bycol, oncols):
    cols = []
    if oncols == False:
        cols = df.columns.to_list ()
    else:
        cols = bycol + oncols
    xdf = df[cols]
    return xdf


def ls2str(ls):
    st = ""
    for i in range (len (ls)):
        if st == "" and ls[i] not in st:
            st = ls[i]
        else:
            st = st + "," + ls[i]
    return st


def pupd(col, val):
    lscol = col.split (',')
    lsval = val.split (',')
    if len (lscol) == len (lsval):
        x1 = ls2str (lscol)
        x2 = ls2str (lsval)
        x = "(" + x1 + ") values (" + x2 + ")"
        return x

def dtype_match(db, table, conn, ndf):
    df = ndf
    #df = ndf.apply(lambda x: x.str.replace("'",''))
    dbcols = []
    dbcolType = []
    try:
        dfy = pd.read_sql("select 1 from " + table, con=conn)
    except:
        x = CreateTable_MSSQL(ndf, table, conn)
    try:
        qry = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "' ORDER BY ORDINAL_POSITION"
        dfx = pd.read_sql(qry, con= conn)
        dbcols = dfx['COLUMN_NAME'].to_list()
        dbcolType = dfx['DATA_TYPE'].to_list()
    except:
        qry = 'EXPLAIN ' + db + '.' + table
        dfx = pd.read_sql(qry, con= conn)
        dbcols = dfx['Field'].to_list()
        dbcolType = dfx['Type'].to_list()
    dc= zip(dbcols, dbcolType)
    dic = dict(dc)
    dfcol = df.columns.to_list()
    dbcols.sort()
    dfcol.sort()
    st = ""
    q = 0
    if dbcols == dfcol:
        comment1 = 'column counts matched exactly'
    else:
        comment1 = 'column counts are not same'
    try:
        for i in range(len(dbcols)):
            dbty = get_key(dic, dbcols[i])
            st = dbcols[i]
            Y = 0
            try:
                xdf = df[st]
                Y = 1
            except:
                Y = 0
            if Y == 1:
                if 'int' in dbty:
                    df[st] = df[st].astype(int)
                elif 'float' in dbty:
                    df[st] = df[st].astype(float)
                elif 'datetime' in dbty or 'timestamp' in dbty:
                    df[st] = df.apply(lambda x : pd.to_datetime(x[st]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
                elif dbty == 'date':
                    df[st] = df.apply(lambda x : pd.to_datetime(x[st]).strftime("%Y-%m-%d"), axis = 1)
                q = q + 1
        return df
    except:
        print(comment1, '-', 'error occuruced for dbcols: ', st , ' at position ', q)

def inser_or_update(db, conn, tbl, df, bycol, oncols=False, exe = False , operator=False):
    ddf = dtype_match(db, tbl, conn, df)
    ndf = attempt_dt(ddf)
    #cr = conn.cursor ()
    udf = forupdate (ndf, bycol, oncols)
    dfx = drop_cols (ndf, bycol)
    ncols = dfx.columns.to_list ()
    lsqry = []
    lsinsert = []
    q = 0
    qq = []
    k = 0
    for i in range (len (ndf)):
        x = ''
        y = ''
        xu = ''
        yu = ''
        for j in range (len (bycol)):
            if operator == False:
                x1 = str (bycol[j]) + " Like '" + str (ndf.loc[i, bycol[j]]) + "'"
            else:
                x1 = str (bycol[j]) + " ='" + str (ndf.loc[i, bycol[j]]) + "'"
            if x == '':
                x = x1
                xu = bycol[j]
                yu = "'" + str (ndf.loc[i, bycol[j]]) + "'"
            else:
                xu = xu + ',' + bycol[j]
                yu = yu + "," + "'" + str (ndf.loc[i, bycol[j]]) + "'"
                x = x + " and " + x1
        for n in range (len (ncols)):
            if oncols == False:
                a1 = str (ncols[n])
                a2 = "'" + str (ndf.loc[i, ncols[n]]) + "'"
                if y == '':
                    y = a1 + '=' + a2
                    xu = xu + ',' + a1
                    yu = yu + "," + a2
                else:
                    y = y + "," + a1 + '=' + a2
                    xu = xu + ',' + a1
                    yu = yu + "," + a2
            else:
                a1 = str (ncols[n])
                mat = 0
                for j in range (len (oncols)):
                    if oncols[j] == a1:
                        mat = 1
                        break
                if mat == 1:
                    a2 = "'" + str (ndf.loc[i, ncols[n]]) + "'"
                    if y == '':
                        y = a1 + '=' + a2
                        xu = xu + ',' + a1
                        yu = yu + "," + a2
                    else:
                        y = y + "," + a1 + '=' + a2
                        xu = xu + ',' + a1
                        yu = yu + "," + a2
        qryinsert = "insert into " + tbl + pupd (xu, yu)
        qry = "update " + tbl + ' set ' + y + ' Where ' + x
        lsqry.append (qry)
        lsinsert.append (qryinsert)
        if exe == True:
            try:
                cr.execute (qry)
            except:
                try:
                    cr.execute (qryinsert)
                except:
                    qq.append (q)
                    pass
        else:
            print(qry, chr(10), chr(10))
            print(qryinsert, chr(10))
            k = k + 1
            if k == 10:
                exit()
        q = q + 1
    print ("failed rows: ", qq)
    ddf = pd.DataFrame (list (zip (lsqry, lsinsert)), columns=['upd', 'ins'])
    return ddf

def df2sq(df, table, conn, bycol=False, oncol=False, operator='Like'):
    if bycol == False and oncol == False:
        df.to_sql(table, con=conn, if_exists="append", chunksize=10000)
        print('success')
    else:
        cr = conn.cursor ()
        try:
            cr.execute ("select 1 from " + table)
            dfx = inser_or_update (conn, table, df, bycol, oncol, operator)
            return dfx
        except:
            df.to_sql (table, con=conn, if_exists="replace", chunksize=10000)
            print('success')



