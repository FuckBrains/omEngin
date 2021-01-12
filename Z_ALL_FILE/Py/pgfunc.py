import os
import time
from datetime import date
import requests as rs
import pyodbc

def served_check(flname,ussd):
    fo = open(flname,"r+")
    txt = fo.read()
    fo.close()
    if ussd in txt:
        return "old"
    else:
        return "new"

def served_entry(flname,ussd):
    fo = open(flname,"a")
    ussdmod = "," + ussd
    txt = fo.write(ussdmod)
    fo.close()

def db_insert_pgrun(ussd,code,mobile):
    socdb = "Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    conx = pyodbc.connect(socdb)
    curs = conx.cursor()
    in_query = "INSERT INTO dbo.pglog1 ( SMSID, SITECODE, MSISDN) VALUES (" + ussd + "," + code + "," + mobile + ");"
    curs.execute(in_query)
    curs.commit
    conx.close()
    print('sql table updated')

def filepath(flname):
    t = time.localtime()
    today = date.today()
    Name1 = today.strftime('%m%d%y')
    Name2 = time.strftime("%H%M", t)
    filenameExt = Name1 + "_" + Name2
    filepaths = os.getcwd() + "\\smple_download\\" + flname + filenameExt + ".csv"
    return filepaths

def sendsms(msisdn,txt):
    sURL1 = "http://10.101.11.164:10144/cgi-bin/sendsms?user=tester&pass=foobar&to="
    sURL2 = "&from=10144&text="
    sURL_pgon = sURL1 + msisdn + sURL2 + txt
    resp = rs.get(sURL_pgon)
    print(resp)

def txt_readbyline(ms, filepath):
    cnt = 0
    mstype = isinstance(ms,str)
    if mstype == False:
        ms = str(ms)
    with open(filepath) as f:
        for line in f:
            ln = line.strip()
            if ln in ms:
                cnt += 1
    return cnt