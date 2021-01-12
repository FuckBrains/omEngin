import pandas as pd
import numpy as np
import os


def drop_cols(df, col2drop=[]):
    if len (col2drop) > 0:
        cols = df.columns.to_list ()
        ncols = []
        for i in range (len (cols)):
            match = 0
            for j in range (len (col2drop)):
                if cols[i] == col2drop[j]:
                    match = 1
            if match == 0:
                ncols.append (cols[i])
        ndf = df[ncols]
        return ndf
    else:
        return df


def forupdate(df, bycol, oncols):
    cols = []
    if oncols == False:
        cols = df.columns.to_list ()
    else:
        cols = bycol + oncols
    xdf = df[cols]
    return xdf


def ls2str(ls):
    st = ""
    for i in range (len (ls)):
        if st == "" and ls[i] not in st:
            st = ls[i]
        else:
            st = st + "," + ls[i]
    return st


def pupd(col, val):
    lscol = col.split (',')
    lsval = val.split (',')
    if len (lscol) == len (lsval):
        x1 = ls2str (lscol)
        x2 = ls2str (lsval)
        x = "(" + x1 + ") values (" + x2 + ")"
        return x

def qbuilt(ndf, tbl, bycol, oncols=False, operator=True):
    udf = forupdate (ndf, bycol, oncols)
    dfx = drop_cols (ndf, bycol)
    ncols = dfx.columns.to_list ()
    lsqry = []
    lsinsert = []
    q = 0
    qq = []
    for i in range (len (ndf)):
        x = ''
        y = ''
        xu = ''
        yu = ''
        for j in range (len (bycol)):
            if operator == False:
                x1 = str (bycol[j]) + " Like '" + str (ndf.loc[i, bycol[j]]) + "'"
            else:
                x1 = str (bycol[j]) + " ='" + str (ndf.loc[i, bycol[j]]) + "'"
            if x == '':
                x = x1
                xu = bycol[j]
                yu = "'" + str (ndf.loc[i, bycol[j]]) + "'"
            else:
                xu = xu + ',' + bycol[j]
                yu = yu + "," + "'" + str (ndf.loc[i, bycol[j]]) + "'"
                x = x + " and " + x1
        for n in range (len (ncols)):
            if oncols == False:
                a1 = str (ncols[n])
                a2 = "'" + str (ndf.loc[i, ncols[n]]) + "'"
                if y == '':
                    y = a1 + '=' + a2
                    xu = xu + ',' + a1
                    yu = yu + "," + a2
                else:
                    y = y + "," + a1 + '=' + a2
                    xu = xu + ',' + a1
                    yu = yu + "," + a2
            else:
                a1 = str (ncols[n])
                mat = 0
                for j in range (len (oncols)):
                    if oncols[j] == a1:
                        mat = 1
                        break
                if mat == 1:
                    a2 = "'" + str (ndf.loc[i, ncols[n]]) + "'"
                    if y == '':
                        y = a1 + '=' + a2
                        xu = xu + ',' + a1
                        yu = yu + "," + a2
                    else:
                        y = y + "," + a1 + '=' + a2
                        xu = xu + ',' + a1
                        yu = yu + "," + a2
        qryinsert = "insert into " + tbl + pupd (xu, yu)
        qry = "update " + tbl + ' set ' + y + ' Where ' + x
        lsqry.append (qry)
        lsinsert.append (qryinsert)
    ddf = pd.DataFrame (list (zip (lsqry, lsinsert)), columns=['upd', 'ins'])
    return ddf

df = pd.read_csv(os.getcwd() + "\\ss.csv")
df = df.astype(str)
x = qbuilt(df,'xyz',['SERIAL'])
x.to_csv(os.getcwd() + "\\ssbuilt.csv")


