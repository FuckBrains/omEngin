import pandas as pd
import cx_Oracle, pyodbc, requests, os, time
from mysql import *
from sqlalchemy import create_engine
import omsql.omsqlfn as fn
import omsql.InsUpd as fni
from datetime import *

def sql_between_days(d1 = None, d2 = None):
    print("d1 set to today and d2 set to yesterday")
    nw = datetime.now()
    thisdy = ''
    sincedy = ''
    if d1 == None:
        thisdy = nw.strftime("%Y%m%d")
    else:
        thisdy = d1
    if d2 == None:
        sincedy = ''
    else:
        sincedy = ''

def tm():
    nw = datetime.now()
    thistm = nw.strftime("%Y%m%d_%H%M%S")
    return thistm

def wrt2txt(flpath, content):
    try:
        f = open(flpath, 'a+')
        f.write(content)
        f.close()
        print('print from wrt2txt, *success*', flpath, chr(10))
    except:
        lastslash = flpath.rfind('\\')
        flname = flpath[-lastslash :len(flpath)-4]
        print(flname)
        os.system("taskkill /F /FI '"+ flname + "' /T")
        time.sleep(2)
        try:
            f = open(flpath, 'a+')
            f.write(content)
            f.close()
            print('print from wrt2txt, *success*', flpath, chr(10))
        except:
            print('def wrt2txt *failed* ', flpath, chr(10))

def save_cmd(content):
    nw = datetime.now()
    thisdy = nw.strftime("%Y%m%d")
    thistm = nw.strftime("%Y%m%d_%H%M%S")
    fl = os.getcwd() + '\\' + thisdy + '.txt'
    cont = ''
    try:
        if content == None:
            cont = "class initiated - " + thistm + chr(10)
            wrt2txt(fl, cont)
        elif content == '':
            pass
        else:
            cont = content + ' - ' + thistm + chr(10)
            wrt2txt(fl, cont)
    except:
        print('failed to def save_cmd')

def SaveToCsv(df, content = None, path_with_filename = None):
    pth = ''
    if path_with_filename == None:
        pth = os.getcwd() + '\\' + tm() + '.csv'
    else:
        pth = path_with_filename
    if content == None:
        try:
            df.to_csv(pth, index = False)
            print("save 'df' successfully: ", pth)
        except:
            print('could not saved to path : ', pth)
    else:
        try:
            content.to_csv(pth, index = False)
            print("save 'content' successfully: ", pth)
        except:
            print('could not saved to path : ', pth)

def SaveToText(self, content, path_with_filename = None):
    if path_with_filename == None:
        pth = os.getcwd() + '\\' + tm() + '.txt'
    else:
        pth = path_with_filename
    try:
        wrt2txt(pth, content)
    except:
        print('failed to write in text')

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

##### Class Starts #########

class osql:
    def __init__(self, conn, table, db = None):
        



    

    def is_table_exist(self, tbl):
        qry = "SELECT TOP 3 * FROM " + tbl
        try:
            rs = cur.execute(qry)
            print('table exist')
            return 1
        except:
            print('table does not exist')
            return 0

    def CheckExist(self, tbl, colname, values, args_qry = None):
        qry = ''
        msg = ''
        rw = 0
        if args_qry == None:
            qry = "select * from " + tbl + " where " + colname + "='" + values + "'"
        else:
            qry = "select * from " + tbl + " where " + args_qry + ' and ' + colname + "='" + values + "'"
        cmd = qry
        TS()
        try:
            df = pd.read_sql(qry, conn)
            rw = df.shape[0]
            msg = 'execution success'
        except:
            rw = 'NA'
            msg = 'execution Failed'
        print(qry,' ',  msg,' ', rw)
        return rw

    def Ex(self, arg, return_type = 'dataframe'):
        TS(arg)
        if return_type == 'dataframe':
            print('return datatype will be dataframe')
            try:
                rs = pd.read_sql(arg, con = conn)
                return rs
            except:
                print('execution failed, need to check query string')
        elif return_type == 'fetchone' or return_type == 'row':
            print('return datatype will be rows object')
            try:
                rs = cur.execute(arg)
                return rs
            except:
                print('execution failed, need to check query string')

    def Getdf(self):
        return df

    def setdf(self, ndf):
        df = ndf
        print('dataframe set to df')

    
    
    def InsertSingle(self, tbl, colname, values):
        cmd = "insert into " + tbl + ' ' + fn.prep_insert(colname,values)
        print('qry string from insert: ', cmd)
        try:
            cur.execute(cmd)
            conn.commit()
            print('insert success')
        except:
            print('error')

    def InsertBulk(self, tbl, dataframe , cols = [], condcols = []):
        if len(cols) == 0 and len(condcols) == 0:
            Upd_or_Insert(tbl, dataframe)
        elif len(cols) == 0 and len(condcols) !=0:
            Upd_or_Insert(tbl, dataframe, condcols)
        elif len(cols) != 0 and len(condcols) !=0:
            print(' built required')
            Upd_or_Insert(tbl, dataframe, condcols, cols)

    def UpdateSingle(self, tbl, listcols, listvalue, bycol, bycolv):
        cmd = ''
        x = CheckExist(tbl, bycol, bycolv)
        if x != 0 :
            cmd = "update " + tbl + ' set ' + fn.prep_update(listcols,listvalue) + ' where ' + bycol + "='" + bycolv + "'"
            TS()
            print('Existing rows found, proceed for insert', cmd)
        else:
            cmd = "update " + tbl + ' set ' + fn.prep_insert(listcols,listvalue)
            print('no existing value found, proceed for inserting \n', cmd)
        cur.execute(cmd)
        conn.commit()

    #def df_to_sql(df, tbl = None, cols = ['all_cols_of_df'], how = 'append', replaceby = []):
    def UpdateBulk(self, ndf, tbl, bycond_colname, oncols = False):
        if ndf == False:
            ndf = df
        if oncols:
            try:
                xdf = ndf[oncols]
                ndf = xdf
                Upd_or_Insert(tbl, ndf, bycond_colname)
            except:
                print('def UpdateBulk- oncols mustbe list by u provide ', type(oncols))
                print('update execution halted')

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
            dfx = pd.read_sql(qry, con= engine)
        except:
            cur.execute(qry)
            dfx = pd.DataFrame(cur.fetchall())
        df = dfx

    def DeleteByCond(self, tbl, col, cond):
        xx = "DELETE FROM " + tbl + " WHERE " + col + " Like '" + cond + "'"
        print(xx)
        cur.execute(xx)
        conn.commit()

    def DeleteDuplicate(self, tbl, cond_col):
        qry = "delete t1 FROM " + tbl + " t1 INNER JOIN "+ tbl + " t2 where t1.SL < t2.SL and t1." + cond_col + " = t2." + cond_col
        print(qry)
        cur.execute(qry)
        conn.commit()

    def csv2sql(self, csvfile, tblname, table_cols = 'csvhead', table_dtype = 'TEXT', by_cond_cols = False):
        if isinstance(csvfile, str):
            ndf = pd.read_csv(csvfile)
            df = ndf.apply(lambda x: x.str.replace("'",''))
        else:
            ndf = csvfile
            df = ndf.apply(lambda x: x.str.replace("'",''))
        xx = is_table_exist(tblname)
        if xx == 0:
            xdf = mod_cols_name(df)
            df = xdf
            if table_cols == 'csvhead' or table_cols == 'dataframe_head':
                cols = df.columns.to_list()
            else:
                cols = table_cols
            try:
                if isinstance(table_dtype, str):
                    CreateTable(tblname,cols,None)
                elif isinstance(table_dtype, list) and len(table_dtype) == len(cols):
                    CreateTable(tblname,cols,table_dtype)
                else:
                    print('table cols and table_dtype field not same')
                    exit()
            except:
                print(tabledetails)
        if by_cond_cols:
            Upd_or_Insert(tblname,df, by_cond_cols)
        else:
            Upd_or_Insert(tblname,df)

    def df2sql(self, tblname, ndf, table_cols = 'dataframe_head', table_dtype = 'TEXT', by_cond_cols = False):
        if by_cond_cols:
            csv2sql(ndf, tblname, table_cols, table_dtype, by_cond_cols)
        else:
            csv2sql(ndf, tblname, table_cols, table_dtype)

    def df_tosql(self, df, tblname, oncols = False, bycols = False):
        if is_table_exist(tblname) == 1:
            Upd_or_Insert(self, df, tblname, oncols, bycols)
        

    
def MySql(user, password, host):
    constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
    engine = create_engine(constr, echo=False)
    conn = engine.raw_connection()
    cur = conn.cursor()
    server = 'mysql'
    print('mysql conn successful')

def MsSql(user, password, host):
    cstr = "Driver={SQL Server};SERVER=" + host + ";DATABASE=" + db + ";UID=" + user + ";PWD=" + password
    TS(cstr)
    conn = pyodbc.connect(cstr)
    cur = conn.cursor()
    server = 'mssql'
    print('mssql conn success')

def Oracle(user, password):
    oHost = 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd'
    db = 'SEMDB'
    conn = cx_Oracle.connect(user, password, oHost)
    server = 'oracle'
    print(conn.version)
