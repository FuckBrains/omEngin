import pandas as pd
import os
import omsqlfn as ofn
import InsUpd as InUp
import cx_Oracle, pyodbc, requests, os, time
from mysql import *
from sqlalchemy import create_engine

def get_key(my_dict, val):
    for value, key in my_dict.items():
        if value == val:
            return key

def dtype_match(db, table, conn, ndf):
    df = ndf.apply(lambda x: x.str.replace("'",''))
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
                elif 'float' in dbty:
                    df[st] = df[st].astype(float)
                elif 'datetime' in dbty or 'timestamp' in dbty:
                    df[st] = df.apply(lambda x : pd.to_datetime(x[st]).strftime("%Y-%m-%d %H:%M:%S"), axis = 1)
                elif dbty == 'date':
                    df[st] = df.apply(lambda x : pd.to_datetime(x[st]).strftime("%Y-%m-%d"), axis = 1)
                q = q + 1
        return df
    except:
        print(comment1, '-', 'error occuruced for dbcols: ', st , ' at position ', q)


def Insert_bydf(tbl, df, cols = False):
    q = 0
    if cols:
        ls = []
        for i in range(len(df)):
            lsval = []
            q = q + 1
            for j in df:
                match = 0
                for c in range(len(cols)):
                    if cols[c] == j:
                        match = 1
                        break
                if match == 1:
                    lsval.append(df.loc[i,j])
            qry = "insert into " + tbl + ' ' + ofn.prep_insert(cols,lsval)
            ls.append(qry)
        return ls
    else:
        colname = df.columns.to_list()
        q = 0
        ls = []
        for i in range(len(df)):
            lsval = []
            q = q + 1
            for j in df:
                lsval.append(df.loc[i,j])
            qry = "insert into " + tbl + ' ' + ofn.prep_insert(colname,lsval)
            ls.append(qry)
        return ls

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
    qry = "select * from " + tbl + " where " + colname + "='" + values + "'"
    dfx = pd.read_sql(qry, conn)
    rw = dfx.shape[0]
    return rw

def UPIN(df, tbl, conn, bycols, oncols = False, operation = "and"):
    cr = conn.cursor()
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
            dfx = pd.read_sql(qr, conn)
            rw = dfx.shape[0]
            ls = []
            if rw != 0:
                for n in range(len(fcols)):
                    ls.append(df.loc[i, fcols[n]])
                qry = "update " + tbl + ' set ' + ofn.prep_update(fcols,ls) + ' where ' + x
            else:
                for n in range(len(fcols_pbycol)):
                    ax = df.loc[i, fcols_pbycol[n]]
                    ls.append(ax)
                qry = "insert into " + tbl + ' ' + ofn.prep_insert(fcols_pbycol,ls)
            cr.execute(qry)
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
                qry = "update " + tbl + ' set ' + ofn.prep_update(fcols,ls) + ' where ' + bycols + "='" + condval + "'"
            else:
                for c1 in ndf:
                    ls.append(ndf.loc[i,c1])
                ls.append(condval)
                qry = "insert into " + tbl + ' ' + ofn.prep_insert(fcols_pbycol,ls)
            print(qry)
            cr.execute(qry)
            lsqry.append(qry)
        conn.commit()
        print('update done for ', len(lsqry), ' rows ')
        return lsqry

def UpdInsert(ndf, tbl, conn, bycols = False, oncol = False):
    qry = ''
    cr = conn.cursor()
    if bycols != False and oncol != False:
        qry = qrybuilt(tbl,ndf, bycols, oncol)        
    elif bycols != False and oncol == False:
        qry = qrybuilt(tbl,ndf, bycols, oncol)
    elif bycols == False and oncol != False:
        qry = Insert_bydf(tbl, ndf, oncol)
    else:
        qry = Insert_bydf(tbl, ndf)
    cnt = 0
    for i in range(len(qry)):
        cnt = cnt + 1
        q = qry[i]
        cr.execute(q)
    conn.commit()
    print(cnt, ' rows of data instered into ', tbl)
    return qry


def MySql(user, password, host, db):
    constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
    engine = create_engine(constr, echo=False)
    conn = engine.raw_connection()
    return conn


#df1 = pd.read_sql('select * from omdb3',conn)
#print('before row: ', df1.shape[0])
#pt = os.getcwd() + '\\omsql\\OM2.csv'
#df = pd.read_csv(pt)
#oncol = ['Zone', 'Commercial_Zone', 'PFM_ZONE']
#bycol = ['Code','Authority']
#ls = UPIN(df, 'omdb3', conn, "Code", oncol)
#ls = UPIN(df, 'omdb3', conn, bycol, oncol)
#print('after row: ', df1.shape[0])

# df = dataframe
# db_connection  = database connection object
# how = 'append' or 'replace' or 'tuncate'
# bycols (list/str) = conditional columns for insert and update [if how = 'replace']
# oncols (list) = columns on that update and insert perfromed on table [False = all dataframe column]
# datatype_map = special feat, before insert or update datatype mapping beteen table columns and dataframe columns
def df_to_sql(df, db_name, db_table, db_connection, how = 'replace', bycols = False, oncols = False, datatype_map = True):
    ndf = dtype_match(db_name, db_table, db_connection, df)
    if bycols != False and how == 'replace':
        ls = UPIN(ndf, db_table, db_connection, bycols, oncols)
    elif bycols == False:
        ls = UpdInsert(ndf, db_table, db_connection, bycols = bycols, oncol = oncols)

def pattern1():
    conn = MySql('root','admin','127.0.0.1:3306','omdb')
    pt = os.getcwd() + '\\omsql\\OMDB.csv'
    df = pd.read_csv(pt)
    df_to_sql(df, 'omdb', 'mytable', conn)
    conn.close()

def pattern2():
    print('pattern2')
    conn = MySql('root','admin','127.0.0.1:3306','omdb')
    pt = os.getcwd() + '\\omsql\\OM.csv'
    df = pd.read_csv(pt)
    df_to_sql(df, 'omdb', 'mytable', conn, bycols = ['Code'], oncols = ['Zone','Commercial_Zone','PFM_ZONE'])





#x1 = UpdInsert('TB1',df)
#x2 = UpdInsert('TB1',df, bycol, oncol)
#x3 = UpdInsert('TB1',df, bycol)
#x4 = UpdInsert('TB1',df, oncol = oncol)
#print('X1', '~~', x1)
#print('X2', '~~', x2)
#print('X3', '~~', x3)
#print('X4', '~~', x4)
