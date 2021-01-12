import pandas as pd
import cx_Oracle, pyodbc, requests, os
from mysql import *
from sqlalchemy import create_engine
import OmSQ.omsqlfn as fn
import OmSQ.InsUpd as fni
import os

def about():
        print('initiate class: ', "x = omsql('root','admin','127.0.0.1:3306','omdb')")
        print('Class method : connection ', "MySql(self), MsSql(Self), def Oracle(self)")
        print('class method* : col_and_type(self, table)', 'Return dictionary key as colname and type as value')
        print('class method : Query(self, tbl, colname = False, condition = False)')
        print('class method : Update(self, tbl, listcols, listvalue, bycol, bycolv):')
        print('class method : Insert(self, tbl, colname, values):')
        print('class method : GetResult(self)',' returns dataframe')
        print('class method : SaveToCsv(self, path_with_filename):')
        print('class method : SaveToCsv(self, path_with_filename):')
        print('class method* : Ex(self, arg = False, ret_type = False)', ' for any custom type execution')
        print('every will save at self.df every time and get with GetResult')
        print('few calling example are below in source file')
        print("class Method: CreateTable(self, tablename, list_col, list_type = None, servername = 'mysql')")


def for_contacts(svpt, tblname, colhead):
    fl = open(svpt, 'r+')
    ls = []
    for i in fl.readlines():
        x = i.strip('\n')
        ls.append(x)
    df = pd.DataFrame(ls, columns=[colhead])
    df = df.astype(str)
    col = df.columns.to_list()
    #x = omsql('root','admin','127.0.0.1:3306','omdb')
    x = omsql('sa','Robi456&', '192.168.88.121', 'SOC_Roster')
    x.MsSql()
    x.CreateTable(tblname,col, None,'mssql')
    print(x.col_and_type(tblname))
    x.Export(tblname,df)

def corp_db(csvpt, tblname, colhead):
    df = pd.read_csv(csvpt)
    df = df.astype(str)
    col = df.columns.to_list()
    #x = omsql('root','admin','127.0.0.1:3306','omdb')
    x = omsql('sa','Robi456&', '192.168.88.121', 'SOC_Roster')
    x.MsSql()
    x.CreateTable(tblname, col, None,'mssql')
    print(x.col_and_type(tblname))
    x.Export(tblname,df)

class omsql:
    def __init__(self, User, Password, Host = False, Db = False):
        self.db = Db
        self.user = User
        self.password = Password
        self.host = Host
        self.engin = ''
        self.conn = ''
        self.cur = ''
        self.rw = 0
        self.qry = ''
        self.colv_coltyp = {}
        self.df = pd.DataFrame([''])
        self.dftemp = pd.DataFrame([''])
    def MySql(self):
        constr = 'mysql+mysqlconnector://' + self.user + ':' + self.password + '@' + self.host + '/' + self.db
        print('mysql+mysqlconnector://' + self.user + ':' + self.password + '@' + self.host + '/' + self.db)
        try:
            self.engine = create_engine(constr, echo=False)
            self.conn = self.engine.raw_connection()
            self.cur = self.conn.cursor()
            print('mysql conn successful')
        except:
            print('mysql conn failed')
    def MsSql(self):
        cstr = "Driver={SQL Server};SERVER=" + self.host + ";DATABASE=" + self.db + ";UID=" + self.user + ";PWD=" + self.password
        print(cstr)
        try:
            self.conn = pyodbc.connect(cstr)
            self.cur = self.conn.cursor()
            print('mssql conn success')
        except:
            print('mssql conn failed')
    def Oracle(self):
        oHost = 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd'
        self.db = 'SEMDB'
        self.conn = cx_Oracle.connect(self.user, self.password, oHost)
        print(self.conn.version)
    
    def Ex(self, arg = False, ret_type = False):
        print('ret_type = dataframe/(fetchone/row), so get data as object')
        if arg != False:
            if ret_type == 'dataframe' or ret_type == 'df':
                rs = pd.read_sql(arg, con = self.conn)
                return rs
            elif ret_type == 'fetchone' or ret_type == 'row':
                self.cur.execute(arg)
                rs = self.cur.fetchall()
                return rs
            elif ret_type == False:
                cr = self.conn.cursor()
                cr.execute(arg)
                cr.commit()
        else:
            if self.qry != '':
                try:
                    self.cur.execute(self.qry)
                    self.conn.commit()
                    print('qry executed')
                except:
                    print('error')
    
    def Query(self, tbl, colname = False, condition = False):
        qry = "select * from " + tbl
        if colname != False:
            cname = str(colname)
            if condition == False:
                qry = "select " + cname + " from " + tbl
            else:
                cond = str(condition)
                qry = "select " + cname + " from " + tbl + " where " + cond
        print('query: ', qry)
        try:
            dfx = pd.read_sql(qry, con= self.engine)
        except:
            self.cur.execute(qry)
            dfx = pd.DataFrame(self.cur.fetchall())
        self.df = dfx
    
    def CheckExist(self, tbl, colname, values):
        qry = "select * from " + tbl + " where " + colname + "='" + values + "'"
        print(qry)
        dfx = pd.read_sql(qry, self.conn)
        rw = dfx.shape[0]
        return rw
    
    def Update(self, tbl, listcols, listvalue, bycol, bycolv):
        self.qry = ''
        x = self.CheckExist(tbl, bycol, bycolv)
        if x != 0 :
            self.qry = "update " + tbl + ' set ' + fn.prep_update(listcols,listvalue) + ' where ' + bycol + "=" + bycolv
            print('Existing rows found, proceed for insert', self.qry)
        else:
            self.qry = "update " + tbl + ' set ' + fn.prep_insert(listcols,listvalue)
            print('no existing value found, proceed for inserting \n', self.qry)
        self.cur.execute(self.qry)
        self.conn.commit()
    
    def Insert(self, tbl, colname, values):
        self.qry = "insert into " + tbl + ' ' + fn.prep_insert(colname,values)
        print('qry string from insert: ', self.qry)
        try:
            self.cur.execute(self.qry)
            self.conn.commit()
            print('insert success')
        except:
            print('error')

    def col_and_type(self, table):
        qry = 'EXPLAIN ' + self.db + '.' + table
        dfx = pd.read_sql(qry, con= self.engine)
        cols = dfx['Field'].to_list()
        typ = dfx['Type'].to_list()
        zips = zip(cols, typ)
        self.colv_coltyp = dict(zips)
        return self.colv_coltyp

    def SaveToCsv(self, path_with_filename):
        try:
            self.df.to_csv(path_with_filename, index = False)
            print('save successfully: ', path_with_filename)
        except:
            print('could not saved to path : ', path_with_filename)

    def GetResult(self):
        dfx = self.df
        return dfx

    def Export(self, tbl, dfx, delim = False):
        try:
            dfx.to_sql(name = tbl, con = self.conn, if_exists='replace', index = False)      
        except:
            cols = list(self.colv_coltyp.keys())
            cnt = 0
            insertrow = 0
            for i in range(len(dfx)):
                ls = []
                x = ""
                cnt = cnt + 1
                for j in dfx:
                    x = dfx.loc[i,j]
                    if x == '':
                        ls.append('NA')
                    else:
                        ls.append(dfx.loc[i,j])
                qry = "insert into " + tbl + ' ' + fn.prep_insert(cols,ls)
                print(qry)
                self.cur.execute(qry)
                insertrow = insertrow + 1
            self.conn.commit()
            print('row inserted: ' + str(insertrow))

    def CreateTable(self, tablename, list_col, list_type = None, servername = 'mysql'):
        print('list_col = list of columns, servername can be = mysql/mssql')
        st = ""
        finalstr = ''
        x = ""
        if servername.lower() == 'mssql':
            for i in range(len(list_col)):
                if list_type != None:
                    x = list_col[i] + "' " + list_type[i]
                else:
                    x = list_col[i] + "' TEXT NULL"
                if st == "":
                    st = "CREATE TABLE '" + tablename + "' ( '" + x
                else:
                    st = st + ', ' +  "'" + x
            else:
                finalstr = st + ' )'
                print(finalstr)
                self.cur.execute(finalstr)
                self.conn.commit()
        elif servername.lower() == 'mysql':
            for i in range(len(list_col)):
                if list_type != None:
                    x = list_col[i] + "` " + list_type[i]
                else:
                    x = list_col[i] + "` TEXT NULL"
                if st == "":
                    st = "CREATE TABLE `" + tablename + "` ( `" + x
                else:
                    st = st + ', ' +  "`" + x
            else:
                finalstr = st + ' ) ENGINE=InnoDB'
                print(finalstr)
                self.cur.execute(finalstr)
                self.conn.commit()
    def Upd_or_Insert(self, tbl, ndf, bycols = False):
        if bycols:
            fni.InsertUpdate(self.db, tbl, self.conn, ndf, bycols)
        else:
            fni.InsertUpdate(self.db, tbl, self.conn, ndf)
    def Close(self):
        self.conn.close()

def csv2sql(csvfile, tblname, by_cond_cols = False , table_cols = 'csvhead', table_dtype = 'TEXT' , dbname = None, host = None, user = None, password = None, dbserver = 'mysql'):
    x = ''
    df = pd.read_csv(csvfile)
    if dbname == None:
        x = omsql('root','admin','127.0.0.1:3306','omdb')
    else:
        x = omsql(user, password, host, dbname)
    if dbserver == 'mysql':
        x.MySql()
    else:
        x = dbserver
    if table_cols == 'csvhead':
        cols = df.columns.to_list()
    else:
        cols = table_cols
    try:
        if isinstance(table_dtype, str):
            x.CreateTable(tblname,cols,None,'mysql')
        elif isinstance(table_dtype, list) and len(table_dtype) == len(cols):
            x.CreateTable(tblname,cols,table_dtype,'mysql')
        else:
            print('table cols and table_dtype field not same')
            exit()
    except:
        print('table already exist')
    print(x.col_and_type(tblname))
    if by_cond_cols:
        x.Upd_or_Insert(tblname,df, by_cond_cols)
    else:
        x.Upd_or_Insert(tblname,df)
    return x

S2 = os.getcwd() + '\\SQL\\OmSQ\\bk1.csv'
ob = csv2sql(S2,'test_time')
S0 = os.getcwd() + '\\SQL\\s0.csv'
S1 = os.getcwd() + '\\SQL\\s1.csv' 
#ob = csv2sql(S0,'ONA_2')
#ob = csv2sql(S1,'ONA_2', 'CustomAttr11')

#x.upin('ARABI_2',df, "CustomAttr15")
#SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&
#print(x.Ex("selct * from omdb.ARABI_2", 'df'))

#print(x.CheckExist('sitedb','Site_Code','sfsdgsdfgdfgdsg'))
#x.CreateTable('oTest1',col,coltype,'mysql')
#x = omsql('SOC_READ', 'soc_read')
#x.Oracle()
#x.Query('sitedb')
#print(x.GetResult())
#rs = x.Ex('select * from sitedb', 'row')
#fn.fetchone_read(rs)
#df = x.Ex('select * from sitedb', 'df')
#print(df)


#x = omsql('root','admin','127.0.0.1:3306','omdb')
#x.MySql()
#x.Ex('select * from sitedb')
#print(x.col_and_type('ABC'))
#C1 = ['A1','A2','A3']
#V1 = ['X1','X2','X3']
#C2 = ['A1','A3']
#V2 = ['OMI','ARABI']
#x.Insert('ABC',C1,V1)
#x = omsql('root','admin','127.0.0.1:3306','omdb')
#x.MySql()
#x.Update('ABC',C2,V2,"A2","'ONA'")
#x.Ex('select * from ABC')
#x.column_details('sitedb')
#x.Query('sitedb','LTE_Status', "LTE_Status = 'NA'")
###x.SaveToCsv(svpt)