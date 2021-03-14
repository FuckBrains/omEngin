import pandas as pd
import os, datetime, time, pyodbc
from mysql import *
from sqlalchemy import create_engine
import sq_o_.df_to_sql as insupd
import sq_o_.tbl_mysql as myq
import sq_o_.tbl_mssql as msq

def mssql_115():
    cstr = "Driver={SQL Server};SERVER=192.168.0.115;DATABASE=SOC_Roster;UID=sa;PWD=1q2w3eaz$"
    conn = pyodbc.connect(cstr)
    return conn

def MsSql(user = 'sa', password = '1q2w3eaz$', host = '192.168.0.102:1433', db = "master"):
    cstr = "Driver={SQL Server};SERVER=" + host + ";DATABASE=" + db + ";UID=" + user + ";PWD=" + password
    conn = pyodbc.connect(cstr)
    return conn

def mysql_self(user = 'root', password = 'root', host = '127.0.0.1:3306', db = "omdb"):
    constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
    engine = create_engine(constr, echo=False)
    conn = engine.raw_connection()
    return engine

def MySql(user, password, host, db):
    constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
    engine = create_engine(constr, echo=False)
    conn = engine.raw_connection()
    return engine

def insert_data():
    pt = os.getcwd() + "\\csv\\sclick.csv"
    ndf = pd.read_csv(pt)
    conn = MySql('root','admin','127.0.0.1:3306','omdb')
    lser = insupd.df_to_sql(ndf, 'omdb', 'TAX1', conn, oncolumn = 'ALL')
    conn.close()

def update_by_condition():
    conn = MySql('root','admin','127.0.0.1:3306','omdb')
    pt = os.getcwd() + "\\csv\\sclick.csv"
    df = pd.read_csv(pt)
    lser = insupd.df_to_sql(df, 'omdb', 'TAX1', conn, bycolumn=['CustomAttr15'])
    conn.close()

def createtable():
    conn = MySql('root','admin','127.0.0.1:3306','omdb')    
    pt = os.getcwd() + "\\csv\\sclick.csv"
    df = pd.read_csv(pt)
    x = myq.CreateTable_MYSQL(connection = conn, tablename = 'ASAQ', df = df, table_col = False, table_col_datatype = False, space = '_')
    conn.close()

def sql2df(tbl):
    conn = MySql('root','admin','127.0.0.1:3306','omdb')
    qry = 'select * from '+ tbl
    df = pd.read_sql(qry, con = conn)
    return df

createtable()
#conn = MsSql()
#pt = os.getcwd() + "\\sclick2.csv"
#df = pd.read_csv(pt)
#df.to_sql("t13", con = conn)
#msq.CreateTable_MSSQL(df, "t33", conn)
#lser = insupd.df_to_sql(df, 'SOC_Roster', 't22', conn, oncolumn = 'ALL')
#conn.commit()
#
#dfx = pd.read_sql('select * from omtx2', con = conn)
#print(dfx.columns, dfx.dtypes, df.shape[0])
#dfx = pd.read_sql("select * from t22", con=conn)
#print(dfx.columns, dfx)

