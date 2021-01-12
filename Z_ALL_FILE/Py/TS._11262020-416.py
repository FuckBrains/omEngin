import pandas as pd
import numpy as np
import os
import datetime
import cx_Oracle, pyodbc, requests, os, time
from mysql import *
from sqlalchemy import create_engine

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
        s1 = strval.replace("'","\'")
        s2 = s1.replace(":","\:")
        return s2

def col_space_rmv(ndf, space = '_'):
    df = ndf.replace (np.nan, '')
    dfcol = df.columns.to_list()
    for i in range(len(dfcol)):
        try:
            df = df.rename(columns={dfcol[i]:dfcol[i].replace(' ', space)})
        except:
            pass
    return df
        
        
def dtype_match_mysql(db, table, conn, ndf):
    df = ndf.replace (np.nan, '')
    dfcol = df.columns.to_list()
    for i in range(len(dfcol)):
        df = df.rename(columns={dfcol[i]:dfcol[i].replace(' ', '_')})
    dic = mysql_table_colinfo(db, table)
    try:
        colunmatch = []
        q = 0
        Y = 1
        for i in range(len(dbcols)):
            dbty = get_key(dic, dbcols[i])
            st = dbcols[i]
            q = q + 1
            try:
                xdf = df[st]
            except:
                Y = 0
                notmat = 'column not matched: - ' + st
                print(notmat)
            if Y == 1:
                print('dtype_match: ', dbty)
                try:
                    if dbty == 'int':
                        df[st] = df[st].astype(int)
                    elif dbty == 'float':
                        df[st] = df[st].astype(float)
                    elif dbty == 'datetime':
                        df[st] = df.apply(lambda x : pd.to_datetime(x[st]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
                    elif dbty == 'date':
                        df[st] = df.apply(lambda x : pd.to_datetime(x[st]).strftime("%Y-%m-%d"), axis = 1)
                    else:
                        df = df.apply(lambda x: x.replace("'","\'"))
                except:
                    pass
                q = q + 1
        return df
    except:
        print(comment1, '-', 'error occuruced for dbcols: ', st , ' at position ', q)
         
def insert_into_mysql(tbl, tbl_property, lscol, lsval):
    col = ''
    val = ''
    dic = tbl_property
    if isinstance(lscol, list) and isinstance(lsval, list) and len(lscol) == len(lsval):
        for i in range(len(lscol)):
            valmod = ''
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
        qry = "insert into " + tbl + " (" + col + ") values (" + val + ")"
        return qry
    else:
        return ""
                
def MySql(user, password, host, db):
    constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
    engine = create_engine(constr, echo=False)
    conn = engine.raw_connection()
    return conn    



def dtype_match_dbdf(dataframe, table_col_coltype = {}):
    df = dataframe
    dc = table_col_coltype
    for Kycol in dc:
        cname = Kycol
        ctype = dc[Kycol]
        try:
            if 'text' in ctype or 'varchar' in ctype:
                df[cname] = df[cname].fillna('NA')
                df[cname] = df.apply(lambda x: x[cname].replace("'","\'"))
            elif 'int' in ctype:
                df[cname] = df[cname].astype(int)
                df[cname] = df[cname].replace(np.nan, 0)
            elif 'float' in ctype:
                df[cname] = df[cname].astype(float)
                df[cname] = df[cname].replace(np.nan, 0)
            elif 'datetime' in ctype or 'timestamp' in ctype:
                df[cname] = df[cname].replace(np.nan, '')
                df[cname] = df.apply(lambda x : pd.to_datetime(x[cname]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
            elif 'date' in ctype:
                df[cname] = df[cname].replace(np.nan, '')
                df[cname] = df.apply(lambda x : pd.to_datetime(x[cname]).strftime("%Y-%m-%d"), axis = 1)
            elif 'time' in ctype:
                df[cname] = df[cname].replace(np.nan, '')
                df[cname] = df.apply(lambda x : pd.to_datetime(x[cname]).strftime("%H:%M:%S"), axis = 1)
            else:
                pass
        except:
            pass
    return df

def main():
    pt = os.getcwd() + "\\sclick.csv"
    ndf = pd.read_csv(pt)
    xdf = ndf.convert_dtypes()
    conn = MySql('root','admin','127.0.0.1:3306','om1')
    dc = mysql_table_colinfo('om1', 'TAXW3', conn)
    dfn = dtype_match_dbdf(xdf, dc)
    df = col_space_rmv(dfn, "_")
    q = 0
    rwval = []
    colval = df.columns.to_list()
    for (indx, rwseries) in df.iterrows():
        q = q + 1
        if q == 5:
            break
        rwval = rwseries.values.tolist()
        x = insert_into_mysql('TAXW3', dc, colval, rwval)
        print(x)
#main()      
pt = os.getcwd() + "\\sclick.csv"
ndf = pd.read_csv(pt)
xdf = ndf.convert_dtypes()


#conn = MySql('root','admin','127.0.0.1:3306','om1')
#dc = mysql_table_colinfo('om1', 'TAXW3', conn)
#df = dtype_match_dbdf(xdf, dc)