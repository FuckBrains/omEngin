import pandas as pd
import cx_Oracle, pyodbc, requests, os
from mysql import *
#from sqlalchemy import create_engine
import omsqlfn as fn
import os
from datetime import *
import datetime
import time

#user = 'root'
#password = 'admin'
#host = '127.0.0.1:3306'
#db = 'omdb'
#constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
#engine = create_engine(constr, echo=False)
#conn = engine.raw_connection()
#cur = conn.cursor()

def get_key(my_dict, val): 
    for value, key in my_dict.items(): 
        if value == val:
            return key
        
def dtype_match(db, table, conn, df):
    dbcols = []
    dbcolType = []
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
                elif 'datetime' in dbty or 'timestamp' in dbty:
                    df[st] = df.apply(lambda x : pd.to_datetime(x[st]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
                elif dbty == 'date':
                    df[st] = df.apply(lambda x : pd.to_datetime(x[st]).strftime("%Y-%m-%d"), axis = 1)
                q = q + 1
        return df
    except:
        print(comment1, '-', 'error occuruced for dbcols: ', st , ' at position ', q)
            
#df1['LASTOCCURRENCE'] = pd.to_datetime(df1['LASTOCCURRENCE'],format="%d/%m/%y, %H:%M:%S", errors='raise')
#df1['LASTOCCURRENCE'] = df1.apply(lambda x : pd.to_datetime(x.LASTOCCURRENCE).strftime("%d-%m-%Y h:M"), axis = 1)


def ExInsert(tbl, conn, df):
    colname = df.columns.to_list()
    q = 0
    cr = conn.cursor()
    for i in range(len(df)):
        lsval = []
        q = q + 1
        for j in df:
            lsval.append(df.loc[i,j])
        qry = "insert into " + tbl + ' ' + fn.prep_insert(colname,lsval)
        print(qry)
        cr.execute(qry)
    else:
        conn.commit()
        print('row inserted: ' +  str(q))
        return 'row inserted: ' +  str(q)

def CheckExist(conn , tbl, colname, values):
    qry = "select * from " + tbl + " where " + colname + "='" + values + "'"
    dfx = pd.read_sql(qry, conn)
    rw = dfx.shape[0]
    return rw

def InsertUpdate(db, tbl, con, df, bycol = False):
    allcols = df.columns.to_list()
    ndf = dtype_match(db, tbl, con, df)
    if isinstance(ndf, pd.DataFrame):
        cr = con.cursor()
        if bycol == False:
            rv = ExInsert(tbl, con, ndf)
        else:
            dfx = ndf.drop(bycol, 1)
            colsname = dfx.columns.to_list()
            colscond = ndf[bycol].to_list()
            q = 0
            for i in range(len(colscond)):
                vl = colscond[i]
                chk = CheckExist(con, tbl, bycol, vl)
                ls = []
                qry = ''
                if chk != 0:
                    for c1 in dfx:
                        ls.append(dfx.loc[i,c1])
                    qry = "update " + tbl + ' set ' + fn.prep_update(colsname,ls) + ' where ' + bycol + "='" + vl + "'"
                else:
                    for c1 in ndf:
                        ls.append(ndf.loc[i,c1])
                    qry = "insert into " + tbl + ' ' + fn.prep_insert(allcols,ls)
                cr.execute(qry)
                q = q + 1
                if q <3:
                    print(qry)
                con.commit()
                


