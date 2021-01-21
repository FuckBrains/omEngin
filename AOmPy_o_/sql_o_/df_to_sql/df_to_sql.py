import pandas as pd
import numpy as np
import os
import datetime
import cx_Oracle, pyodbc, requests, os, time
from mysql import *
from sqlalchemy import create_engine
import df_to_sql.upin as upd
import df_to_sql.write2text as wrt

def get_server_name(db, table, conn):
    try:
        qry = 'EXPLAIN ' + db + '.' + table
        dfx = pd.read_sql(qry, con = conn)
        return "MYSQL"
    except:
        try:
            qry = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "' ORDER BY ORDINAL_POSITION"
            dfx = pd.read_sql(qry, con= conn)
            return "MSSQL"
        except:
            return "only MYSQL and MSSQL is Supported"

def mssql_table_colname(db, table, conn):
    qry = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "' ORDER BY ORDINAL_POSITION"
    dfx = pd.read_sql(qry, con = conn)
    dbcols = dfx['COLUMN_NAME'].to_list()
    return dbcols

def mssql_table_colinfo(db, table, conn):
    qry = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "' ORDER BY ORDINAL_POSITION"
    dfx = pd.read_sql(qry, con = conn)
    dbcols = dfx['COLUMN_NAME'].to_list()
    dbcolType = dfx['DATA_TYPE'].to_list()
    dc= zip(dbcols, dbcolType)
    dic = dict(dc)
    return dic

def mysql_table_colname(db, table, conn):
    qry = 'EXPLAIN ' + db + '.' + table
    dfx = pd.read_sql(qry, con = conn)
    dbcols = dfx['Field'].to_list()
    return dbcols

def mysql_table_colinfo(db, table, conn):
    qry = 'EXPLAIN ' + db + '.' + table
    dfx = pd.read_sql(qry, con = conn)
    dbcols = dfx['Field'].to_list()
    dbcolType = dfx['Type'].to_list()
    dc= zip(dbcols, dbcolType)
    dic = dict(dc)
    return dic



def get_key(my_dict, val):
    for value, key in my_dict.items():
        if value == val:
            return key
            
def modstr(strval):
    if isinstance(strval, str):
        s1 = strval.replace("'","\\'")
        s2 = s1.replace(":","\\:")
        return s2

def dtype_match_dbdf(dataframe, table_col_coltype = {}):
    df = dataframe
    dc = table_col_coltype
    for Kycol in dc:
        cname = Kycol
        ctype = dc[Kycol]
        try:
            if 'text' in ctype or 'varchar' in ctype:
                pass
            elif 'int' in ctype:
                df[cname] = df[cname].astype(int)
            elif 'float' in ctype:
                df[cname] = df[cname].astype(float)
            elif 'datetime' in ctype or 'timestamp' in ctype:
                df[cname] = df.apply(lambda x : pd.to_datetime(x[cname]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
            elif 'date' in ctype:
                df[cname] = df.apply(lambda x : pd.to_datetime(x[cname]).strftime("%Y-%m-%d"), axis = 1)
            elif 'time' in ctype:
                df[cname] = df.apply(lambda x : pd.to_datetime(x[cname]).strftime("%H:%M:%S"), axis = 1)
            else:
                pass
        except:
            pass
    return df

def fuzzymatch(str1,str2, uplow = True):
    if uplow == True:
        s1 = str1.lower()
        s2 = str2.lower()
        ls1 = []
        ls2 = []
        for i in s1:
            ls1.append(i)
        for n in s2:
            ls2.append(n)
        q = 0
        succ = 0
        fail = 0
        if len(ls1) <= len(ls2):
            for j in range(len(ls1)):
                q = q + 1
                if ls1[j] == ls2[j]:
                    succ = succ + 1
                else:
                    fail = fail + 1
        else:
             for j in range(len(ls2)):
                q = q + 1
                if ls1[j] == ls2[j]:
                    succ = succ + 1
                else:
                    fail = fail + 1
        try:
            spercent = round((succ/q)*100,2)
        except:
            spercent = 0
        return spercent

def colchk_dbdf(coldb = [], coldf = []):
    if isinstance(coldb, list) and isinstance(coldf, list):
        cdb = coldb
        cdf = coldf
        cdb.sort
        coldf.sort
        nonmat = []
        for i in range(len(cdb)):
            d1 = cdb[i]
            mat = 0
            for j in range(len(cdf)):
                if d1 == cdf[j]:
                    mat = 1
                    break
            if mat == 0:
                nonmat.append(d1)
        return nonmat

def insert_into_sql(tbl, tbl_property, lscol, lsval):
    col = ''
    val = ''
    dic = tbl_property
    if isinstance(lscol, list) and isinstance(lsval, list) and len(lscol) == len(lsval):
        for i in range(len(lscol)):
            valmod = ''
            try:
                if lsval[i] != '' and lsval[i] is not None:
                    dtype = get_key(dic,lscol[i])
                    if dtype == 'text' or dtype == 'varchar':
                        valmod = modstr(lsval[i])
                    else:
                        valmod = str(lsval[i])
                    if val == '':
                        col = lscol[i]
                        val = "'" + valmod + "'"
                    else:
                        col = col + ',' + lscol[i]
                        val = val + ',' + "'" + valmod + "'"
                else:
                    pass
            except:
                pass
        qry = "insert into " + tbl + " (" + col + ") values (" + val + ")"
        return qry
    else:
        return ""


def df_to_sql(dataframe, dbname, tablename, conn, oncolumn = "ALL", bycolumn = None, opeation = 'and'):
    srv = get_server_name(dbname, tablename, conn)
    print(srv)
    if srv == 'other':
        exit()
    cr = conn.cursor()
    try:
        cr.execute('select 1 from '+ tablename)
    except:
        print('table does not exits')
        exit()
    if oncolumn != 'ALL' and bycolumn == None:
        dataframe = dataframe[oncolumn]
    ndf = dataframe.replace(r'^\s*$', np.nan, regex=True)
    xdf = ndf.convert_dtypes()
    dfcol = xdf.columns.to_list()
    if srv == "MYSQL":
        dbcol = mysql_table_colname(dbname, tablename, conn) #function call
    elif srv == "MSSQL":
        dbcol = mssql_table_colname(dbname, tablename, conn) #function call
    nonmat = colchk_dbdf(dbcol,dfcol)
    dfc = []
    rnmcol = {}
    if len(nonmat) != 0:
        for n in range(len(nonmat)):
            dbc = nonmat[n]
            y = 0
            for i in range(len(dfcol)):
                x = fuzzymatch(dbc, dfcol[i])
                #print(dbc,' - ',  dfcol[i], ' p- ', x, ' max ', y)
                if x >= y:
                    y = x
                    dfcl = dfcol[i]
            else:
                dfc.append(dfcl)
                rnmcol[dfcl] = dbc
    xdf = xdf.rename(columns = rnmcol)
    if srv == "MYSQL":
        dc = mysql_table_colinfo(dbname, tablename, conn)  #mysql function call
    elif srv == "MSSQL":
        dc = mssql_table_colinfo(dbname, tablename, conn)  #mysql function call
    df = dtype_match_dbdf(xdf, dc) #function call
    if bycolumn == None:
        excmd = []
        q = 0
        rwval = []
        colval = df.columns.to_list()
        er = []
        for (indx, rwseries) in df.iterrows():
            q = q + 1
            rwval = rwseries.values.tolist()
            x = insert_into_sql(tablename, dc, colval, rwval)
            try:
                cr.execute(x)
                excmd.append(x)
            except:
                er.append(x)
                qq = "dfrow: " + str(q)
                er.insert(0, qq)
        print('row inserted: ', q - len(er), ' error found for rows: ', len(er), ", get error in return")
        wrt.wrt2txt(excmd, 'exe_succ')
        wrt.wrt2txt(excmd, 'exe_fail')
        return er
    else:
        tableprop = dc
        excmd = upd.UPIN(df, tablename, tableprop, conn, bycols = bycolumn, operations = 'and')
        wrt.wrt2txt(excmd, 'exe_succ')




