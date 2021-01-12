import time
from datetime import date
from datetime import datetime
from datetime import timedelta
import pyodbc
import requests as rs
import pandas as pd

tmnw = datetime.now()
qryst = tmnw.strftime('%Y-%m-%d %H:%M:%S')

def general_qry():
    conx = pyodbc.connect('Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&')
    qry = "SELECT * from [dbo].[pglog4]"
    df = pd.read_sql(qry, conx)
    print(df)
    print(df.shape[0])

def sms(msisdn,txt):
    sURL1 = "http://10.101.11.164:10144/cgi-bin/sendsms?user=tester&pass=foobar&to="
    sURL2 = "&from=10144&text="
    sURL_pgon = sURL1 + msisdn + sURL2 + txt
    resp = rs.get(sURL_pgon)
    print(resp)

def db_insert_pgon(ussd,code,msisdn):
    conx = pyodbc.connect('Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&')
    curs = conx.cursor()
    in_qry = '''INSERT INTO dbo.pglog4 (SMSID, SITECODE, MSISDN) VALUES (?,?,?)'''
    in_qry_1 = (ussd, code, msisdn)
    curs.execute(in_qry, in_qry_1)
    conx.commit()
    sms(str(msisdn),"PGSTART ACK AT " + qryst + " CODE:" + code)
    conx.close()

def db_query_duplicate(code):
        conx = pyodbc.connect('Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&')
        curs = conx.cursor()
        select_qry = "SELECT * FROM pglog4 WHERE SITECODE = '"+ code +"' AND STATUS_ACTIVE= 'TRUE'"
        curs.execute(select_qry)
        rows = curs.fetchone()
        bol = bool(rows)
        if bol == True:
            return "ACT_CASE_FOUND"
            conx.close()
        else:
            return "NO_ACT_CASE"
            conx.close()

def db_update_pgoff(code,msisdn):
    conx = pyodbc.connect('Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&')
    curs = conx.cursor()
    qry_1 = "(SITECODE = '" + code + "' AND  MSISDN = " + msisdn + " AND STATUS_ACTIVE= 'TRUE')"
    #qryussd = "SELECT SMSID FROM pglog4 WHERE " + qry_1
    #ussd = curs.execute(qryussd)
    qry1 = "UPDATE dbo.pglog4 SET END_DATETIME = CURRENT_TIMESTAMP WHERE " + qry_1
    qry2 = "UPDATE dbo.pglog4 SET CASE_STATUS = 'Closed' WHERE " + qry_1
    curs.execute(qry1)
    conx.commit()
    curs.execute(qry2)
    conx.commit()
    qry_2 = "(SITECODE = '" + code + "' AND  MSISDN = " + msisdn + " AND CASE_STATUS= 'Closed')"
    qry3 = "UPDATE dbo.pglog4 SET STATUS_ACTIVE = '0' WHERE " + qry_2
    curs.execute(qry3)
    conx.commit()
    #print(ussd)
    #ftime = "SELECT START_DATEDATE FROM pglog4 WHERE SMSID = ? "
    #st = curs.execute(ftime,ussd)
    #pgruntime = 'From' + qryst + ' To '+ str(st)
    conx.close()
    sms(msisdn,"PGSTOP ACK AT " + qryst + ' Code: '+ code)

def main(ussd,code,msisdn,job):
    if job == "PGSTART":
        ans = db_query_duplicate(code)
        if ans == "NO_ACT_CASE":
            db_insert_pgon(str(ussd),code,str(msisdn))
            return 'PGON_DONE'
        else:
            #sms(msisdn,"PGSTART ALREADY LOGGED (Duplicate Entry)")
            return ""
    elif job == "PGSTOP":
        ans = db_query_duplicate(code)
        if ans == "ACT_CASE_FOUND":
            db_update_pgoff(code,str(msisdn))
            return 'PGOFF_DONE'
        else:
            #sms(msisdn,"NO PGON Found, Invalid PG OFF Request")
            return ""

#db_insert_pgon('223','DHGUL36','8801817184338')
#x = db_query_duplicate('PBSDR11')
#print(x)
#general_qry()
