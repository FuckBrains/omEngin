from datetime import *
import time as tm
import pandas as pd
import numpy as np
import os
from mysql import *
from sqlalchemy import create_engine
from sqlalchemy import update
import subprocess
import conn_brocker

engine = create_engine('mysql+mysqlconnector://akomi:1q2w3eaz$@38.70.234.101:3306/omdb', echo=False)
conn = engine.raw_connection()
cursor = conn.cursor()

def dbup(df0):
    df1 = df0.applymap(str)
    ls = ['ip','port','ISP','ASN','country','prot','prot_status','blkchk','blk_status','priority']
    df2 = df1[ls]
    df2.to_sql(name='live', con=engine, if_exists = 'append', index=False)
    print('db update done')

def dbdw():
    qry = "select * from live where blk_status='fine'"
    df = pd.read_sql(qry, con=engine)
    return df
    
def dbupd(ip):
    live.update().where(ip==ip).values(name="some name")
    
    
def netcat(ip,port):
    qry = "timeout 5 nc -v -N  -w 5 " + ip + ' ' + str(port)
    process = subprocess.Popen(qry, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    err = str(process.stderr.read())
    if 'succeeded' in err:
        return 'success'
    else:
        return 'fail'

def getip_port():
    df = dbdw()
    print(df)
    df = df.fillna(0)
    df2 = df[df.blk_status.str.contains('fine')]
    df2 = df2.drop_duplicates(subset='ip',keep='last', inplace = False)
    dfp1 = df2[(df2.priority == "P1")]
    dfp2 = df2[(df2.priority != "P1")]
    ippr = ""
    for i in range(len(dfp1)):
        ip = dfp1.iloc[i][1]
        port = dfp1.iloc[i][2]
        status = netcat(ip,port)
        if status == 'success':
            nip = ip
            nport = port
            ippr = ip + "," + str(port)
            break
    return ippr

def connint():
    ipr = getip_port()
    ipx = ipr.split(',')
    nip = ipx[0]
    nport = ipx[1]
    conn_brocker.server('0.0.0.0', 12876, nip, int(nport), 2)



nip = '142.93.245.242'
nport = '30588'
conn_brocker.server('0.0.0.0', 12876, nip, int(nport), 2)


