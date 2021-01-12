import pandas as pd
import numpy as np
import os
import MySQLdb
import csv
import requests
import io
import datetime as dt


def api_hideme():
    hideme_access = "730480402242392"
    hideme = "http://incloak.com/api/proxylist.php?out=csv&code=" + hideme_access
    urlData = requests.get(hideme).content
    df = pd.read_csv(io.StringIO(urlData.decode('utf-8')),delimiter=";")
    return df

conn= MySQLdb.connect("23.152.224.49","akomi","1q2w3eaz$","omdb")
cur = conn.cursor()
pt = os.getcwd()
proxy = pt + '\\hideme.csv'
db = pt + '\\kkk.csv'
db2 = pt + '\\i2a.csv'
qqq = pt + '\\QQQQAQA.csv'

def split_col(npary,splitby,colname):
    rw, col = npary.shape
    flist = []
    for i in range(rw):
        lst = []
        x = npary[i][0]
        lst = x.split(splitby)
        lst.insert(0,x)
        flist.append(lst)
    df = pd.DataFrame(flist,columns = colname)
    return df

def add_col(dA,c):
    rw, col = dA.shape
    lst = []
    for i in range(rw):
        x = dA[i][c]
        y = x.rfind('.')
        s = x[0:y]
        lst.append(s)
    dA = np.append(dA, np.array([lst]).transpose(), axis=1)
    return dA

def api_ip2asn(ip):
    url = "https://api.iptoasn.com/v1/as/ip/" + ip
    x = requests.get(url)
    y = x.json()
    as_owner = y["as_description"]
    return as_owner

def missing(z):
    rw, col = z.shape
    lst = []
    dic = {}
    cnt = 0
    for i in range(rw):
        lst2 = []
        lst.append([z[i][0],z[i][1],z[i][2]])
        for c in range(col):
            lst2.append(z[i][c])
        ipmd = z[i][2]
        ip = z[i][0]
        df = ddb[ddb['IPMOD'].str.contains(ipmd)]
        if df.shape[0] > 0:
            rs = api_ip2asn(ip)
            print(rs)
            cnt = cnt + 1
        else:
            print(df)
        dic.update( {i : lst2} )
        if cnt == 5:
            break
    print(dic)
    #for c in range(col):
        #print(z[i][c])
        
    #x = z[i][2]
    #print(x)
   
#df1 = pd.DataFrame(z,columns=['ip','port','IPMOD'])
#print(df1)

#cur = conn.cursor()
#for row in df1():

#dpx = api_hideme()
def nearest(lst, K): 
     lst = np.asarray(lst) 
     idx = (np.abs(lst - K)).argmin() 
     return lst[idx]
    
def df_filering(df,c1,c1val,c2,c2val,c3,c3val):
    df0 = df.loc[(df[c1]==c1val) & (df[c2]==c2val) & (df[c3]==c3val)]
    if df0.shape[0] == 0:
        df0 = df.loc[(df[c1]==c1val) & (df[c2]==c2val)]
        if df0.shape[0] == 0:
            df0 = df.loc[(df[c1]==c1val)]
    return df0

x = 0
if x == 1:
    qq = pd.read_csv(qqq)
    dpx1 = qq[['ip']]
    listformat = ['ip','i1','i2','i3','i4']
    getlist = split_col(dpx1.to_numpy(),".",listformat)
    print(getlist)
    df = pd.DataFrame(getlist,columns = listformat)
    dff = pd.merge(qq,df,on ='ip',how ='left')


def find_owner(nr,ip):
    x = ip.split('.')
    xsum1 = int(x[0]) + int(x[1]) + int(x[2])
    xsum2 = xsum1 + int(x[3])
    rw, col = nr.shape
    rn = []
    mnpre = 10000
    indx = 0
    for r in range(rw):
        I1 = nr[r][14]
        I2 = nr[r][15]
        diff1 = abs(I1 - xsum1)
        #print(I1,xsum1)
        mn = min(diff1,mnpre)
        if mn < mnpre and xsum2<=I2:
            indx = r
            mnpre = mn
    return nr[indx][0]

def filename_maker():
    y = dt.datetime.now()
    x = y.strftime('%d%m%Y-%H%M')
    dww = os.getcwd() + '\\' + x + '.csv'
    return dww
    
proxy = pt + '\\hideme.csv'
db = pt + '\\kkk.csv'
df = pd.read_csv(db)
#qq = pd.read_csv(proxy,delimiter=';')
qq = api_hideme()
dpx1 = qq[['ip','port']]
ls = []
for r in range(dpx1.shape[0]):
    lst = []
    ip = dpx1.iloc[r,0]
    prt = dpx1.iloc[r,1]
    ipx = ip.split('.')
    ddf = df_filering(df,'IP1-B1',int(ipx[0]),'IP1-B2',int(ipx[1]),'IP1-B3',int(ipx[2]))
    aa = find_owner(ddf.to_numpy(),ip)
    lst.insert(0,ip)
    lst.insert(1,prt)
    lst.insert(2,aa)
    ls.append(lst)
    
fdf = pd.DataFrame(ls,columns = ['ip','port','SL'])
ffd = pd.merge(fdf,df,on ='SL',how ='left')
fd = ffd[['ip','port','ISP','ASN','Country']]
fd.to_csv(filename_maker())


p = 0
if p == 1:
    B12 = ddf.columns.get_loc("IP1-B2")
    B22 = ddf.columns.get_loc("IP2-B2")
    B13 = ddf.columns.get_loc("IP1-B3")
    B23 = ddf.columns.get_loc("IP2-B3")
    for r in range(ddf.shape[0]):
        iB12 = ddf.iloc[r,B12]
        iB22 = ddf.iloc[r,B22]
        iB13 = ddf.iloc[r,B13]
        iB23 = ddf.iloc[r,B23]
        B2 = matching_point(50,iB12,iB22)
     
    

#rw, col = npar1.shape
#for i in range(rw):
    #B2_1 = npar1[i]['IP1-B2']
    #B2_2 = npar1[i]['IP2-B2']

#for i,j in getlist:
    #lstt= j
    #lstt.insert(0, i)
    #print(lstt)
#ddb = pd.read_csv(db)
#narr = split_col(ddb.to_numpy(),0)


#ddb2 = pd.read_csv(db2)
#dA = dpx1.to_numpy()
#narr = add_col(dA,0)
#df1 = pd.DataFrame(narr,columns=['ip','port','IPMOD'])
#RJ = pd.merge(df1,ddb2,on ='IPMOD',how ='left')

#missing(narr)

#df = pd.read_sql("select * from ipasn10",conn)
#print(df)
#fdf = df1.merge(df, on='IPMOD')
#fdf.to_csv(pt + '\\merged.csv')
#print(fdf)
