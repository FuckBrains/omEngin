import pandas as pd
import omsqlfn as fn
import pyodbc, requests, os, time
from mysql import *
from sqlalchemy import create_engine

def CheckExist(conn , tbl, colname, values):
    qry = "select * from " + tbl + " where " + colname + "='" + values + "'"
    dfx = pd.read_sql(qry, conn)
    rw = dfx.shape[0]
    return rw

def Update_insert_single(conn, tbl, listcols, listvalue, bycol, bycolv):
    cur = conn.cursor()
    cmd = ''
    x = CheckExist(conn, tbl, bycol, bycolv)
    if x != 0:
        cmd = "update " + tbl + ' set ' + fn.prep_update(listcols, listvalue) + ' where ' + bycol + "='" + bycolv + "'"
        print('Existing rows found, proceed for update', cmd)
    else:
        cmd = "insert into " + tbl + ' ' + fn.prep_insert(listcols, listvalue)
        print('no existing value found, proceed for insert \n', cmd)
    cur.execute(cmd)
    conn.commit()

def Query(conn, tbl = None, Ex = False, colname = False, condition = False):
    cur = conn.cursor()
    if Ex:
        if isinstance(Ex, str):
            df = pd.read_sql(Ex, conn)
            return df
            exit()
    if colname != False and tbl != None:
        x = ''
        qry = ''
        if isinstance(colname, list):
            for i in range(len(colname)):
                if x == '':
                    x = colname[i]
                else:
                    x = x + "," + colname[i]
        else:
            x = str(colname)
        if condition != False:
            y = ''
            if isinstance(condition, list):
                for i in range(len(condition)):
                    if y == '':
                        x = condition[i]
                    else:
                        y = y + " and " + condition[i]
                qry = "select " + x + " from " + tbl + " where " + y
            else:
                y = str(condition)
                qry = "select " + x + " from " + tbl + " where " + y
    print('query: ', qry)
    dfx = pd.read_sql(qry, con= conn)
    return dfx

def DeleteByCond(conn, tbl, col, cond):
    xx = "DELETE FROM " + tbl + " WHERE " + col + " Like '" + cond + "'"
    cur = conn.cursor()
    cur.execute(xx)
    conn.commit()

def DeleteDuplicate(conn, tbl, cond_col):
    qry = "delete t1 FROM " + tbl + " t1 INNER JOIN "+ tbl + " t2 where t1.SL < t2.SL and t1." + cond_col + " = t2." + cond_col
    cur = conn.cursor()
    cur.execute(qry)
    conn.commit()

def MySql(user, password, host, db):
    constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
    engine = create_engine(constr, echo=False)
    conn = engine.raw_connection()
    return conn


conn = MySql('root','admin','127.0.0.1:3306','omdb')
#print(Query(conn, tbl = 'mytable', Ex = "select * from eve"))
#print(Query(conn, tbl = 'mytable', colname = ['Code', 'Zone']))
#print(Query(conn, tbl = 'mytable', colname = ['Code', 'Zone']), condition = " Zone Like 'BAR'")