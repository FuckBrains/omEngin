import pandas as pd
import os, datetime, time, pyodbc
import omsql.df_to_sql as insupd
import omsql.create_table.tbl_mysql as myq
import omsql.create_table.tbl_mssql as msq
from omsql.singlebuilt.single_sql import *
from omsql.write2text import *
from omsql.conn_brocker import *

def MsSql(user = 'root', password = 'admin', host = '127.0.0.1:3306', db = "omdb"):
    cstr = "Driver={SQL Server};SERVER=" + host + ";DATABASE=" + db + ";UID=" + user + ";PWD=" + password
    conn = pyodbc.connect(cstr)
    return conn

def mysql_dell(user = 'root', password = 'admin', host = '127.0.0.1:3306', db = "omdb"):
    constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
    engine = create_engine(constr, echo=False)
    conn = engine.raw_connection()
    return conn

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
    x = myq.CreateTable_MYSQL(connection = conn, tablename = 'TAX2', df = df, table_col = False, table_col_datatype = False, space = '_')
    conn.close()

def sql2df(tbl):
    conn = MySql('root','admin','127.0.0.1:3306','omdb')
    qry = 'select * from '+ tbl
    df = pd.read_sql(qry, con = conn)
    return df

#createtable()
#conn = MySql('root','admin','127.0.0.1:3306','omdb')
#pt = os.getcwd() + "\\OMTX.csv"
#df = pd.read_csv(pt)
#df.to_sql('omtx2', con= conn, if_exists='replace', chunksize= 10000)
#dfx = pd.read_sql('select * from omtx2', con = conn)
#print(dfx.columns, dfx.dtypes, df.shape[0])
#CreateTable_MSSQL(df, tablename, conn) #import create_table.tbl_mssql as msq
#x = myq.CreateTable_MYSQL(connection = conn, tablename = 'TAX2', df = df, table_col = False, table_col_datatype = False, space = '_')
#lser = insupd.df_to_sql(dataframe=df, dbname='omdb', tablename='TAX1', conn = conn, oncolumn = "ALL", bycolumn=['CustomAttr15'], opeation = 'and')

def update_table(dataframe, db, tbl, con, bycolumn_list):
    lser = insupd.df_to_sql(dataframe, db, tbl, con, oncolumn = 'ALL' , bycolumn=bycolumn_list)
    return lser

def create_table(df, tablename, con, server = 'mssql'):
    xx = ''
    if server == 'mssql':
        xx = msq.CreateTable_MSSQL(df, tablename, con)
    elif server == 'mysql':
        xx = myq.CreateTable_MYSQL(con, tablename, df, table_col = False, table_col_datatype = False, space = '_')
    else:
        print('currently only mysql and mssql')
    return xx

def create_table_custom(tbl, conn, list_col, list_type = False, server = "mssql"):
    if server == 'mssql':
        msq.CT_MSSQL(conn, tbl, list_col, list_type)
    elif server =='mysql':
        myq.CreateTable_MYSQL(conn, tbl, list_col, list_type , space = '_')
        
def upload_bulkdata(df, tablename, conn, dbname):
    try:
        df.to_sql(name=tablename, con=conn, if_exists='append', chunksize=10000)
        ls = df.columns.to_list()
        DeleteDuplicate(conn, tablename, ls[0])
    except:
        try:
            lser = insupd.df_to_sql(df, dbname, tablename, conn, oncolumn = 'ALL')
            print(lser)
        except:
            print('failed')
    
def update_single(con, tbl, listcols = [], listvalue = [], bycol = '', bycolv='' ):
    xx = Update_insert_single(con, tbl, listcols, listvalue, bycol, bycolv)
    return xx

#DeleteDuplicate(conn, tbl, cond_col)

def HELP():
    c1 = """update_single(con, tbl, listcols = [], listvalue = [], bycol = '', bycolv='' )"\n,
"return connection -> mssql_121(), mysql_dell()\n"
"upload_bulkdata(df, tablename, conn, dbname)"\n,
"create_table_custom(tbl, conn, list_col, list_type = False, server = "mssql")"\n,
"create_table(df, tablename, con, server = 'mssql')"\n,
"update_table(dataframe, db, tbl, con, bycolumn_list)","Sample:\n,
"obj.df_to_sql(dataframe=df, dbname='omdb', tablename='TAX1', conn = conn, oncolumn = "ALL", bycolumn=['CustomAttr15'], opeation = 'and')"""
    print(c1)          
    



    
    
    
    
