import pandas as pd
import os
from datetime import *
import numpy as np
from fn import *
from mysql import *
from sqlalchemy import create_engine

def MySql(user='root',password='admin',host='127.0.0.1:3306',db='om2'):
    constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
    engine = create_engine(constr, echo=False)
    conn = engine.raw_connection()
    return engine

pt = os.getcwd () + "\\"
def text2list(pth):
    f = open (pth, 'r+')
    ls = []
    for i in f.readlines ():
        ls.append (i.replace ('\n', ''))
    return ls

def oFn1(df, *argv, **kwargs):
    ls = []
    col = df.columns.to_list()
    for n in range(len(argv)):
        TempLs = df[argv[n]].values.tolist()
        if len(ls) == 0:
            ls = TempLs
        else:
            tls = [i + j for i, j in zip(ls, TempLs)]
            ls = tls
    ld = []
    for key,value in kwargs.items():
        if col.count(value) != 0:
            TmpLd = df[value].to_list()
            if len(ld) == 0:
                ld = TmpLd
            else:
                tld = [i + j for i, j in zip(ld, TmpLd)]
                ld = tld
        else:
            ar = np.full(df.shape[0], value)
            TmpLd = ar.tolist()
            if len(ld) == 0:
                ld = TmpLd
            else:
                tld = [i + j for i, j in zip(ld, TmpLd)]
                ld = tld
    fls = []
    for i in range(len(ld)):
        x = ls.count(ld[i])
        fls.append(x)
    colx = 'C' + str(df.shape[1])
    df[colx] = np.array(fls)
    return df


def text2dic(pth):
    f = open (pth, 'r+')
    dc = {}
    for i in f.readlines ():
        a1 = i.replace ('\n', '')
        a2 = a1.split (':')
        dc[a2[0]] = a2[1]
    return dc


def getkey(my_dict, ky):
    if ky is not None:
        for key, value in my_dict.items ():
            if key in str (ky):
                return value
        else:
            return "other"


DURCAT = lambda x : '<2H' if (x < 120) \
                else ('<12H' if (x < 240) \
                else ('<6H' if (x < 360) \
                else ('<12H' if (x < 720) \
                else ('<24H' if (x < 1440) \
                else ('48H+')))))

class fluc:
    def __init__(self, mdf):
        self.df = mdf
        self.col = mdf.columns.to_list()
        self.tdf = pd.DataFrame([])
    def initial(self, df):
        db = os.getcwd () + "\\OMDB.csv"
        dfdb = pd.read_csv(db)
        semcol = os.getcwd () + "\\semcols.txt"
        cat = os.getcwd () + "\\catdef.txt"
        df0 = df.rename(columns=str.upper)
        ls = text2list(semcol)
        df1 = df0[ls]
        dc = text2dic(cat)
        df1 = df1.assign(cat = "0")
        df1 = df1.assign(Code = "0")
        df1['cat'] = df1.apply (lambda x: getkey (dc, x.SUMMARY), axis=1)
        df1['Code'] = df1.apply (lambda x: x.CUSTOMATTR15[0:5], axis=1)
        df2 = df1.merge(dfdb, on='Code')
        df2['LASTOCCURRENCE'] = pd.to_datetime (df2['LASTOCCURRENCE'], errors='coerce')
        #df2['CLEARTIMESTAMP'] = pd.to_datetime(df2['CLEARTIMESTAMP'], errors='coerce')
        df2['DUR'] = df2.apply (lambda x: abs (datetime.now () - x['LASTOCCURRENCE']), axis=1)
        #df2['DUR'] = df2['DUR'].astype ('timedelta64[m]')
        #df2['DURCAT'] = df2.apply (lambda x: DURCAT (x.DUR), axis=1)
        #df2['LO'] = df2.apply(lambda x : pd.to_datetime(x['LASTOCCURRENCE']).strftime("%y%m%d%H%M"), axis = 1)
        #df2['CDLO'] = df2['LO'].str.cat(df2['CUSTOMATTR15'])
        #df3 = df2[~df2['cat'].isin(['other'])]
        #df4 = df3[['SERIAL','CUSTOMATTR3','CUSTOMATTR5','OUTAGEDURATION','EQUIPMENTKEY','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','cat','Code','Zone','DUR','DURCAT','CDLO']]
        #print(df4[['LASTOCCURRENCE']])
        df2.to_csv(pt + "\\A10.csv")


df1 = pd.read_csv(pt + 'OMT1.csv')
x = fluc(df1)
x.initial(df1)
#conn = MySql()
#df = pd.read_csv (pt + 'A10.csv')
#df.to_sql('adb10', con = conn, if_exists='replace', chunksize= 10000)

#dfx = df.groupby(['CUSTOMATTR15','DURCAT'])['DURCAT'].count()
#print(df[['DURCAT']])
#df1 = countifs(df,df['CUSTOMATTR15'],df['CUSTOMATTR15'],df['DURCAT'],'48H+')
#print(df1)
#df1.to_csv(pt + "\\A6.csv")