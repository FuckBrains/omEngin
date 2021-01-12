import time
from datetime import date
from datetime import datetime
from datetime import timedelta
import pyodbc
import requests as rs
import pandas as pd

tmnw = datetime.now()
qryst = tmnw.strftime('%Y-%m-%d %H:%M:%S')

def generalqry():
    conx = pyodbc.connect('Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&')
    qry = "SELECT * from [dbo].[pglog4]"
    df = pd.read_sql(qry, conx)
    print(df)
    print(df.shape[0])

def insert_pgon(ussd,code,msisdn):
    conx = pyodbc.connect('Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&')
    curs = conx.cursor()
    in_qry = '''INSERT INTO dbo.pglog4 (SMSID, SITECODE, MSISDN) VALUES (?,?,?)'''
    in_qry_1 = (ussd, code, msisdn)
    curs.execute(in_qry, in_qry_1)
    conx.commit()
    conx.close()

def update_pgoff(code,msisdn):
    conx = pyodbc.connect('Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&')
    curs = conx.cursor()
    qry_1 = "(SITECODE = '" + code + "' AND  MSISDN = " + msisdn + " AND STATUS_ACTIVE= 'TRUE')"
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
    conx.close()