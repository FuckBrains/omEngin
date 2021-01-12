import pandas as pd
import numpy as np
import os
from mysql import *
from sqlalchemy import create_engine

def myFun(arg1, *argv, **kwargs): 
    print ("First argument :", arg1) 
    for arg in argv: 
        print("Next argument through *argv :", arg)

def oFn1(df, *argv, **kwargs):
    #*argv = df column names
    #**kwargs = df columns values
    print(df.columns)
    ls = []
    for n in range(len(argv)):
        TempLs = df[argv[n]].values.tolist()
        if len(ls) == 0:
            ls = TempLs
        else:
            tls = [i + j for i, j in zip(ls, TempLs)]
            ls = tls
            print(ls)
        print(ls)
    #for i in range(len(colname)):
    #print(colname[i])
def AA():
    db = os.getcwd() + "\\OMDB.csv"
    livedb = os.getcwd() + "\\robi_live.csv"
    xa = os.getcwd() + "\\xa.csv"
    sclick = os.getcwd() + "\\sclick.csv"
    df = pd.read_csv(sclick)
    ls_sclick = ['Severity','Node','Resource']
    dc_sclick = {'Severity': 'Severity','Node':'Node','Resource':'Resource'}
    oFn1(df, ls_sclick, one = '1', two = '2', )





def MySql(user = 'root', password = 'admin', host = '127.0.0.1:3306', db = "om2"):
    constr = 'mysql+mysqlconnector://' + user + ':' + password + '@' + host + '/' + db
    engine = create_engine(constr, echo=False)
    conn = engine.raw_connection()
    return engine


pth = os.getcwd() + "\\OMTX.csv"
pth2 = os.getcwd() + "\\OMT.csv"
df = pd.read_csv(pth, low_memory=False)
ndf = df[['SERIAL','IDENTIFIER','NODE','AGENT','ALERTGROUP','SEVERITY','LOCALSECOBJ','X733EVENTTYPE','X733SPECIFICPROB','MANAGEDOBJCLASS','GEOINFO','CUSTOMATTR3','CUSTOMATTR5','CUSTOMATTR25','TTSEQUENCE','TTSTATUS','SRCDOMAIN','CUSTOMATTR26','OUTAGEDURATION','EQUIPMENTKEY','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP']]
ndf.to_csv(pth2)
conn = MySql()
#= ndf.convert_dtypes()
ndf.to_sql("big5", con = conn,  if_exists = 'append', index= False, chunksize=5000)





