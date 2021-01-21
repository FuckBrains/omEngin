import pandas as pd
import os, time
import datetime
from datetime import *

def tm():
    nw = datetime.now()
    thistm = nw.strftime("%Y%m%d_%H%M%S")
    return thistm

def wrt2txt(contents, filename = 'excmd', flpath = None):
    if flpath == None:
        flpath = os.getcwd() + filename + '_' + tm() + '.txt'
    content = "executed commands"
    if isinstance(contents, list):
        for i in range(len(contents)):
            content = content + chr(10) + contents[i]
    else:
        content = contents
    try:
        f = open(flpath, 'w+')
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
            f = open(flpath, 'w+')
            f.write(content)
            f.close()
            print('print from wrt2txt, *success*', flpath, chr(10))
        except:
            print('def wrt2txt *failed* ', flpath, chr(10))

def df_dtype_conv(dfn):
    df = dfn.apply(lambda x: x.replace("'",''))
    ndf = df.convert_dtypes()
    cols = ndf.columns.to_list()
    for i in range(len(cols)):
        col = cols[i]
        if ndf[col].dtypes == 'string':
            try:
                ndf[col] = ndf.apply(lambda x : pd.to_datetime(x[col]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
                ndf[col] = pd.to_datetime(ndf[col])
            except:
                pass
    return ndf

def mysql_lstyp(d_type):
    addID = "NULL DEFAULT NULL"
    if d_type == 'Int64':
        return "INT " + addID
    elif d_type == 'datetime64[ns]':
        return "DATETIME " + addID
    elif d_type == 'Float64':
        return "FLOAT " + addID
    else:
        return "TEXT " + addID

def is_table_exist(tbl, conn):
    qry = "SELECT 1 FROM " + tbl
    try:
        cr = conn.cursor()
        rs = cr.execute(qry)
        print('table already exist')
    except:
        print('table creation failed')

def CreateTable_MYSQL(connection, tablename, df = None, table_col = False, table_col_datatype = False, space = '_'):
    addID = "SL INT AUTO_INCREMENT PRIMARY KEY, "
    addID = ""
    st = ""
    cr = connection.cursor()
    try:
        cr.execute()
    except:
        if table_col != False:
            if table_col_datatype == False:
                typ = 'TEXT NULL DEFAULT NULL'
                for i in range(len(table_col)):
                    x = "`" + table_col[i].replace(' ',space) + "` " + typ
                    if st == "":
                        st = "CREATE TABLE IF NOT EXISTS `" + tablename + "` ( " + addID + x
                    else:
                        st = st + ', ' + x
                return st
            else:
                for i in range(len(table_col)):
                    x = "`" + table_col[i].replace(' ',space) + "` " + table_col_datatype[i]
                    if st == "":
                        st = "CREATE TABLE IF NOT EXISTS `" + tablename + "` ( " + addID + x
                    else:
                        st = st + ', ' + x
                return st
        elif df.shape[0] != 0 and table_col == False:
            xdf = df_dtype_conv(df)
            df = xdf
            table_col = df.columns.to_list()
            for i in range(len(table_col)):
                x = "`" + table_col[i].replace(' ',space) + "` " + mysql_lstyp(df[table_col[i]].dtypes)
                if st == "":
                    st = "CREATE TABLE IF NOT EXISTS `" + tablename + "` ( " + x
                else:
                    st = st + ', ' + x
        else:
            print("please pass, df = True or table_col = True")
            return ""
            exit
        sst = st + ") ENGINE = InnoDB CHARSET=utf32 COLLATE utf32_general_ci"
        print(sst)
        try:
            cr.execute(sst)
            connection.commit()
            print('table creation attempt success if not exist', ' table name ', tablename)
        except:
            print('table creation failed')
            print(sst)
        return sst
        


            
            
            
            