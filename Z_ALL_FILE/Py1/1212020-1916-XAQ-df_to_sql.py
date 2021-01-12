import pandas as pd
import numpy as np
import os
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

def drop_cols(df, col2drop = []):
    if len(col2drop) > 0:
        cols = df.columns.to_list()
        ncols = []
        for i in range(len(cols)):
            match = 0
            for j in range(len(col2drop)):
                if cols[i] == col2drop[j]:
                    match = 1
            if match == 0:
                ncols.append(cols[i])
        ndf = df[ncols]
        return ndf
    else:
        return df


def qrybuilt(tbl, ndf, bycol, oncols = False):
    dfx = drop_cols(ndf, bycol)
    ncols = dfx.columns.to_list()
    lsqry = []
    for i in range(len(ndf)):
        x = ''
        y = ''
        for j in range(len(bycol)):
            x1 = str(bycol[j]) + "='" + str(ndf.loc[i, bycol[j]]) + "'"
            if x == '':
                x = x1
            else:
                x = x + " and " + x1
        for n in range(len(ncols)):
            if oncols == False:
                a1 = str(ncols[n])
                a2 = "'" + str(ndf.loc[i, ncols[n]]) + "'"
                if y == '':
                    y = a1 + '=' + a2
                else:
                    y = y + "," + a1 + '=' + a2
            else:
                a1 = str(ncols[n])
                mat = 0
                for j in range(len(oncols)):
                    if oncols[j] == a1:
                        mat = 1
                        break
                if mat == 1:
                    a2 = "'" + str(ndf.loc[i, ncols[n]]) + "'"
                    if y == '':
                        y = a1 + '=' + a2
                    else:
                        y = y + "," + a1 + '=' + a2
        qry = "update " + tbl + ' set ' + y + ' Where ' + x
        lsqry.append(qry)
    return lsqry

def CheckExist(conn , tbl, colname, values):
    try:
        qry = "select * from " + tbl + " where " + colname + " LIKE '" + values + "'"
        dfx = pd.read_sql(qry, conn)
        rw = dfx.shape[0]
        return rw
    except:
        qry = "select * from " + tbl + " where " + colname + "='" + values + "'"
        dfx = pd.read_sql(qry, conn)
        rw = dfx.shape[0]
        return rw
        

def get_key(my_dict, val):
    for value, key in my_dict.items():
        if value == val:
            return key

def modstr(strval):
    if isinstance(strval, str):
        s1 = strval.replace("'","\\'")
        s2 = s1.replace(":","\\:")
        return s2

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

def prep_update(tbl, tbl_property, lscol,lsval):
    hp = ''
    stval = ''
    dic = tbl_property
    if isinstance(lscol, list) and isinstance(lsval, list):
        if len(lscol) == len(lsval):
            for i in range(len(lscol)):
                try:
                    if lsval[i] is not None and lsval[i] !='':
                        dtype = get_key(dic,lscol[i])
                        if dtype == 'text' or dtype == 'varchar':
                            stval = modstr(lsval[i])
                        else:
                            stval = str(lsval[i])
                        x = lscol[i] + "='" + stval + "'"
                        if hp == '':
                            hp = x
                        else:
                            hp = hp + ',' + x
                    else:
                        pass
                except:
                    pass
        else:
            print('num of col and value are not same')
        return hp
    elif isinstance(lscol, str) and isinstance(lsval, str):
        hp = ""
        comma = lsval.count(',')
        invertcomma = lsval.count("'")
        if invertcomma == (comma+1)*2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            print(x1,x2)
            for i in range(len(x1)):
                x = x1[i] + "=" + x2[i]
                if hp == '':
                    hp = x
                else:
                    hp = hp + ',' + x
        if invertcomma <= 2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            for i in range(len(x1)):
                x = str(x1[i]) + "='" + str(x2[i]) + "'"
                if hp == '':
                    hp = x
                else:
                    hp = hp + ',' + x
        return hp

def UPIN(df, tbl, tblproperty, conn, bycols, oncols = False, operations = "and"):
    cr = conn.cursor()
    er = 0
    lser = []
    if isinstance(bycols, list):
        xdf = None
        bydf = df[bycols]
        ndf = drop_cols(df, bycols)
        if oncols:
            xdf = ndf[oncols]
        else:
            xdf = ndf
        fcols = xdf.columns.to_list()
        fcols_pbycol = xdf.columns.to_list()
        for n in range(len(bycols)):
            fcols_pbycol.append(bycols[n])
        dfup = df[fcols_pbycol]
        x = ''
        #print(fcols, fcols_pbycol, len(fcols), len(fcols_pbycol))
        lsqry = []
        for i in range(len(df)):
            x = ''
            for j in range(len(bycols)):
                lss = bycols[j]
                lsv = df.loc[i,lss]
                st = str(lss) + "='" + str(lsv) + "'"
                if x == '':
                    x = st
                else:
                    x = x + " " + operation + " " + st
            qr = "select * from " + tbl + " where " + x
            try:
                dfx = pd.read_sql(qr, conn)
            except:
                x = qr.find('where ')
                qr0 = qr[0:x]
                qr1 = qr[x:len(qr)]
                qr2 = qr1.replace("=", " LIKE ")
                qrf = qr0 + qr2
                dfx = pd.read_sql(qrf, conn)
                
            rw = dfx.shape[0]
            ls = []
            if rw != 0:
                for n in range(len(fcols)):
                    ls.append(df.loc[i, fcols[n]])
                qry = "update " + tbl + ' set ' + prep_update(tbl, tblproperty, fcols,ls) + ' where ' + x
            else:
                for n in range(len(fcols_pbycol)):
                    ax = df.loc[i, fcols_pbycol[n]]
                    ls.append(ax)
                qry = insert_into_sql(tbl, tblproperty , fcols_pbycol,ls)
            try:
                cr.execute(qry)
            except:
                lser.append(qry)
                er = er + 1
                print('error sql: ', qry)
                if er > 500:
                    wrt2txt(excmd, 'exe_error')
                    print('exiting as error greater than 500 rows')
                    exit()
            lsqry.append(qry)
        conn.commit()
        print('update done for ', len(lsqry), ' rows ')
        return lsqry
    elif isinstance(bycols, str):
        xdf = None
        byc = df[bycols].values.tolist()
        ndf = drop_cols(df, [bycols])
        if oncols:
            xdf = ndf[oncols]
        else:
            xdf = ndf
        fcols = xdf.columns.to_list()
        fcols_pbycol = xdf.columns.to_list()
        fcols_pbycol.append(bycols)
        lsqry = []
        for i in range(len(byc)):
            condval = byc[i]
            rs = CheckExist(conn, tbl, bycols, condval)
            ls = []
            if rs != 0:
                for c1 in xdf:
                    ls.append(xdf.loc[i,c1])
                qry = "update " + tbl + ' set ' + prep_update(tbl, tblproperty, fcols,ls) + ' where ' + bycols + " Like '" + condval + "'"
            else:
                for c1 in ndf:
                    ls.append(ndf.loc[i,c1])
                ls.append(condval)
                qry = insert_into_sql(tbl, tblproperty , fcols_pbycol,ls)
            print(qry)
            cr.execute(qry)
            lsqry.append(qry)
        conn.commit()
        print('update done for ', len(lsqry), ' rows ')
        return lsqry

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
    if isinstance(oncolumn, list) or oncolumn != 'ALL' and bycolumn == None:
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
        wrt2txt(excmd, 'exe_succ')
        wrt2txt(excmd, 'exe_fail')
        return er
    else:
        tableprop = dc
        excmd = UPIN(df, tablename, tableprop, conn, bycols = bycolumn, operations = 'and')
        wrt2txt(excmd, 'exe_succ')




