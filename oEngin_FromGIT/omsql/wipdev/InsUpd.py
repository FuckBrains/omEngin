import pandas as pd
import os, datetime, time
from datetime import *

#user = 'root'
#password = 'admin'
#host = '127.0.0.1:3306'
#db = 'omdb'
#constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
#engine = create_engine(constr, echo=False)
#conn = engine.raw_connection()
#cur = conn.cursor()

def prep_update(lscol,lsval):
    hp = ''
    if isinstance(lscol, list) and isinstance(lsval, list):
        if len(lscol) == len(lsval):
            for i in range(len(lscol)):
                x = str(lscol[i]) + "='" + str(lsval[i]) + "'"
                if hp == '':
                    hp = x
                else:
                    hp = hp + ',' + x
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

def prep_insert(lscol,lsval):
    hp = ''
    if isinstance(lscol, list) and isinstance(lsval, list):
        if len(lscol) == len(lsval):
            ls = []
            for i in range(len(lsval)):
                ls.append("'" + str(lsval[i]) + "'")
                hp = '(' + str.join(',', lscol) + ') values (' + str.join(',', ls) + ')'
        else:
            hp = "check list values for double color"
            print('num of col and value are not same')
        return hp
    elif isinstance(lscol, str) and isinstance(lsval, str):
        hp1 = ""
        hp2 = ""
        hp = ""
        cnt = 0
        comma = lsval.count(',')
        invertcomma = lsval.count("'")
        if invertcomma == (comma+1)*2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            for i in range(len(x1)):
                if hp1 == '':
                    hp1 = str(x1[i])
                    hp2 = str(x2[i])
                    cnt = cnt + 1
                else:
                    hp1 = hp1 + "," + str(x1[i])
                    hp2 = hp2 + "," + str(x2[i])
                    cnt = cnt + 1
                hp = '(' + hp1 + ') values (' + hp2 + ')'
            return hp
        elif invertcomma <= 2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            for i in range(len(x1)):
                if hp1 == '':
                    hp1 = str(x1[i])
                    hp2 = "'" + str(x2[i]) + "'"
                    cnt = cnt + 1
                else:
                    hp1 = hp1 + "," + str(x1[i])
                    hp2 = hp2 + "," + "'" + str(x2[i]) + "'"
                    cnt = cnt + 1
                hp = '(' + hp1 + ') values (' + hp2 + ')'
            return hp

def fetchone_read(rs):
    if isinstance(rs, list):
        print('fetchone readed called \n ')
        ls = []
        cnt = 0
        for r in rs:
            ls1 = list(r)
            cnt = cnt + 1
            print(cnt , '.', ls1)
            ls.append(ls1)
    else:
        print('list type data required but passed data type is ', type(rs))

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
        qry = "insert into " + tbl + ' ' + prep_insert(colname,lsval)
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


def InsertUpdate(db, tbl, con, df, bycol = False, oncols = False):
    allcols = df.columns.to_list()
    ndf = dtype_match(db, tbl, con, df)
    if isinstance(ndf, pd.DataFrame):
        cr = con.cursor()
        if bycol == False:
            rv = ExInsert(tbl, con, ndf)
        else:
            if isinstance(bycol, list):
                if oncols != False:
                    lsqry = qrybuilt(tbl, ndf, bycol, oncols)
                else:
                    lsqry = qrybuilt(tbl, ndf, bycol)
                for i in range(len(lsqry)):
                    qry = lsqry[i]
                    try:
                        cr.execute(qry)
                    except:
                        print("failed lsqry get from 'def qrybuilt' ", qry)
                con.commit()
            elif isinstance(bycol, str):
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
                        qry = "update " + tbl + ' set ' + prep_update(colsname,ls) + ' where ' + bycol + "='" + vl + "'"
                    else:
                        for c1 in ndf:
                            ls.append(ndf.loc[i,c1])
                        qry = "insert into " + tbl + ' ' + prep_insert(allcols,ls)
                    cr.execute(qry)
                    q = q + 1
                    if q <3:
                        print(qry)
                    con.commit()

def InsertUpdate_mod(db, tbl, con, df, bycol = False, oncols = False):
    allcols = []
    if oncols:
        allcols = oncols
    else:
        allcols = df.columns.to_list()
    ndf = dtype_match(db, tbl, con, df)
    if isinstance(ndf, pd.DataFrame):
        cr = con.cursor()
        if bycol == False:
            rv = ExInsert(tbl, con, ndf)
        else:
            if isinstance(bycol, str):
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
                        qry = "update " + tbl + ' set ' + prep_update(colsname,ls) + ' where ' + bycol + "='" + vl + "'"
                    else:
                        for c1 in ndf:
                            ls.append(ndf.loc[i,c1])
                        qry = "insert into " + tbl + ' ' + prep_insert(allcols,ls)
                    cr.execute(qry)
                    q = q + 1
                    if q <3:
                        print(qry)
                    con.commit()
            elif isinstance(bycol, list): # ndf, bycol
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
                        a1 = str(ncols[n])
                        a2 = "'" + str(ndf.loc[i, ncols[n]]) + "'"
                        if y == '':
                            y = a1 + '=' + a2
                        else:
                            y = y + "," + a1 + '=' + a2
                    qry = "update " + tbl + ' set ' + y + ' Where ' + x
                    lsqry.append(qry)
                    print('InsertUpdate_mod qry: ', qry)
                return lsqry
