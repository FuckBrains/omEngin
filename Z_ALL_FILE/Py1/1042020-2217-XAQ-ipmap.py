import pandas as pd
import numpy as np
import os
import MySQLdb
import csv
import requests
import io
import datetime as dt


def filename_maker():
    y = dt.datetime.now()
    x = y.strftime('%d%m%Y-%H%M')
    dww = os.getcwd() + '\\' + x + '.csv'
    return dww

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
        mn = min(diff1,mnpre)
        if mn < mnpre and xsum2<=I2:
            indx = r
            mnpre = mn
    return nr[indx][0]

def api_hideme():
    hideme_access = "730480402242392"
    hideme = "http://incloak.com/api/proxylist.php?out=csv&code=" + hideme_access
    urlData = requests.get(hideme).content
    df = pd.read_csv(io.StringIO(urlData.decode('utf-8')),delimiter=";")
    return df

def df_filering(df,c1,c1val,c2,c2val,c3,c3val):
    df0 = df.loc[(df[c1]==c1val) & (df[c2]==c2val) & (df[c3]==c3val)]
    if df0.shape[0] == 0:
        df0 = df.loc[(df[c1]==c1val) & (df[c2]==c2val)]
        if df0.shape[0] == 0:
            df0 = df.loc[(df[c1]==c1val)]
    return df0

def maincall(df_port_ip):
    df = pd.read_csv(path_ip2as)
    dpx1 = df_port_ip[['ip','port']]
    dpip = df_port_ip[['ip']]
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
        print(ls)
    fdf = pd.DataFrame(ls,columns = ['ip','port','SL'])
    ffd = pd.merge(fdf,df,on ='SL',how ='left')
    fd = ffd[['ip','port','ISP','ASN','Country']]
    print(fd)
    fd.to_csv(filename_maker())
    return fd
