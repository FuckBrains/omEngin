import pandas as pd
import numpy as np
import os

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

def prep_update(lscol,lsval):
    hp = ''
    stval = ''
    if isinstance(lscol, list) and isinstance(lsval, list):
        if len(lscol) == len(lsval):
            for i in range(len(lscol)):
                if lsval[i] is not None:
                    if isinstance(lsval[i],str):
                        xxx1 = lsval[i].replace("'","\\'")
                        stval = xxx1.replace(":","\\:")
                    else:
                        stval = str(lsval[i])
                    x = str(lscol[i]) + "='" + stval + "'"
                    if hp == '' and len(stval) > 0 :
                        hp = x
                    else:
                        if len(stval) > 0:
                            hp = hp + ',' + x
                        else:
                            pass
                else:
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
                qry = "update " + tbl + ' set ' + prep_update(fcols,ls) + ' where ' + x
            else:
                for n in range(len(fcols_pbycol)):
                    ax = df.loc[i, fcols_pbycol[n]]
                    ls.append(ax)
                qry = "insert into " + tbl + ' ' + insert_into_sql(fcols_pbycol,ls)
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
                qry = "update " + tbl + ' set ' + prep_update(fcols,ls) + ' where ' + bycols + "='" + condval + "'"
            else:
                for c1 in ndf:
                    ls.append(ndf.loc[i,c1])
                ls.append(condval)
                qry = "insert into " + tbl + ' ' + insert_into_sql(fcols_pbycol,ls)
            print(qry)
            cr.execute(qry)
            lsqry.append(qry)
        conn.commit()
        print('update done for ', len(lsqry), ' rows ')
        return lsqry