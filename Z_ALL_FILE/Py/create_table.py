import pandas as pd
import os
import cx_Oracle, pyodbc, requests, os, time
from mysql import *
from sqlalchemy import create_engine

def mod_cols_name(df):
    cols = df.columns.to_list()
    sqlkey = ['ADD','ALTER','ALL','AND','ANY',
              'AS','ASC','BETWEEN','CASE','CHECK','COLUMN','CONSTRAINT',
              'CREATE','DATABASE','DEFAULT','DELETE','DESC','DISTINCT','DROP','EXEC','EXISTS','FROM',
              'HAVING','IN','INDEX','JOIN','LIKE','LIMIT','NOT','OR','PROCEDURE',
              'ROWNUM','SELECT','SET','TABLE','TOP','UNION','UNIQUE','UPDATE','VALUES','VIEW','WHERE']
    for i in range(len(cols)):
        st = cols[i]
        stmod = st.replace(' ','_')
        for n in sqlkey:
            if stmod == n:
                xx = '_' + stmod
                stmod = xx
        if st != stmod:
            df = df.rename(columns = {st:stmod})
    return df

def CreateTable_MSSQL(tablename, list_col, list_type=None):
    st = ""
    finalstr = ''
    x = ""
    for i in range(len(list_col)):
        if list_type != None:
            x = list_col[i] + "' " + list_type[i]
        else:
            x = list_col[i] + "' TEXT NULL"
        if st == "":
            addsl = " SL INT PRIMARY KEY IDENTITY (1, 1), "
            st = "CREATE TABLE '" + tablename + "'(" + addsl + "'" + x
            # st = "CREATE TABLE '" + tablename + "' ( '" + x
        else:
            st = st + ', ' + "'" + x
    else:
        finalstr = st + ' )'
        return finalstr

def CreateTable_MYSQL(tablename, list_col, list_type =None):
    st = ""
    finalstr = ''
    x = ""
    for i in range(len(list_col)):
        if list_type != None:
            x = list_col[i] + "` " + list_type[i]
        else:
            x = list_col[i] + "` text NULL DEFAULT NULL"
        if st == "":
            addID = "SL INT AUTO_INCREMENT PRIMARY KEY, "
            st = "CREATE TABLE IF NOT EXISTS `" + tablename + "` ( " + addID + "`" + x
        else:
            st = st + ', ' + "`" + x
    else:
        finalstr = st + ' )'
        print(finalstr)
        return finalstr

def is_table_exist(tbl, conn):
    qry = "SELECT 1 FROM " + tbl
    print(qry)
    try:
        cr = conn.cursor()
        rs = cr.execute(qry)
        return 1
    except:
        return 0

def create_table_mysql(csv_or_df, new_table_name, db_conn, columns_remane = True):
    exist = is_table_exist(new_table_name, db_conn)
    if exist == 0:
        df = ''
        if isinstance(csv_or_df, str):
            df = pd.read_csv(csv_or_df)
        else:
            df = csv_or_df
        if columns_remane:
            ndf = mod_cols_name(df)
            cols = ndf.columns.to_list()
            try:
                qry = CreateTable_MYSQL(new_table_name,cols)
                cr = db_conn.cursor()
                cr.execute(qry)
                db_conn.commit()
                print('table creation successful')
            except:
                print('table creation failed')
        else:
            cols = ndf.columns.to_list()
            CreateTable_MYSQL(new_table_name,cols)
    else:
        print('table already exist')

def create_table_mssql(csv_or_df, new_table_name, db_conn, columns_remane = True, infer_datatype = True):
    if is_table_exist(new_table_name, db_conn) == 0:
        df = ''
        if isinstance(csv_or_df, str):
            df = pd.read_csv(csv_or_df)
        else:
            df = csv_or_df
        if columns_remane:
            ndf = mod_cols_name(df)
            cols = ndf.columns.to_list()
            try:
                qry = CreateTable_MSSQL(new_table_name,cols)
                cr = db_conn.cursor()
                cr.execute(qry)
                db_conn.commit()
                print('table creation successful')
            except:
                print('table creation failed')
        else:
            cols = ndf.columns.to_list()
            CreateTable_MYSQL(new_table_name,cols)
    else:
        print('table already exist')

def MySql(user, password, host, db):
    constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
    engine = create_engine(constr, echo=False)
    conn = engine.raw_connection()
    return conn

def change_dtype(df, colnm, to_type):
    ndf = pd.DataFrame([])
    if to_type == "dt":
        try:
            df[colnm] = df.apply(lambda x : pd.to_datetime(x[colnm]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
            return df
        except:
            return ndf
    if to_type == "integer":
        try:
            df[colnm] = df[colnm].astype(str)
            return df
        except:
            return ndf
    if to_type == "string":
        try:
            df[colnm] = df[colnm].apply(lambda _: str(_))
            return df
        except:
            return ndf


#create_table_mysql(df, 'mytable1', conn)
#qry = "select * from mytable"
#print(pd.read_sql(qry ,con = conn))
