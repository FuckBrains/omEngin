import pandas as pd
import numpy as np
import os
import MySQLdb
import csv
import requests
import io
import datetime as dt
import geoip2.database

#shift-ctrl-b

pt = os.getcwd()
dbmx = "E:\\GIT\\OmProject\\OmSocks\\ippro\\GeoLite2-City.mmdb"
dbas2ip = "E:\\GIT\\OmProject\\OmSocks\\ippro\\ip2asn.csv"

def mxdb(ip):
    with geoip2.database.Reader(dbmx) as reader:
        try:
            response = reader.city(ip)
            lst = response.city.name + ' -' + response.country.iso_code
            return lst
        except:
            lst = 'NA'
            return lst

def filename_maker():
    y = dt.datetime.now()
    x = y.strftime("%d%m%Y-%H%M")
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


def df_filering(df,c1,c1val,c2,c2val,c3,c3val):
    df0 = df.loc[(df[c1]==c1val) & (df[c2]==c2val) & (df[c3]==c3val)]
    if df0.shape[0] == 0:
        df0 = df.loc[(df[c1]==c1val) & (df[c2]==c2val)]
        if df0.shape[0] == 0:
            df0 = df.loc[(df[c1]==c1val)]
    return df0

def maincall(df_port_ip):
    df = pd.read_csv(dbas2ip)
    dpx1 = df_port_ip[['ip','port']]
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
        lst.insert(2,mxdb(ip))
        lst.insert(3,aa)
        ls.append(lst)
    fdf = pd.DataFrame(ls,columns = ['ip','port','CityCountry','SL'])
    ffd = pd.merge(fdf,df,on ='SL',how ='left')
    fd = ffd[['ip','port','CityCountry','ISP','ASN','Country']]
    fd.to_csv(filename_maker())
    return fd
