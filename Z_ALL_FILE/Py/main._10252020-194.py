import pandas as pd
import pyodbc
from datetime import date
from datetime import datetime
from datetime import timedelta
import requests as rs

tday = date.today()
tmdlta = datetime.now() + timedelta(minutes=1)
tmnw = datetime.now() - timedelta(minutes=1)
qryst = tmnw.strftime('%Y-%m-%d %H:%M:%S')
qryend = tmdlta.strftime('%Y-%m-%d %H:%M:%S')

UserEx = "Driver={SQL Server};Server=10.101.4.193;Database=ROC;Uid=om29861;Pwd=Roc@072$123"
conx = pyodbc.connect(UserEx)

def sendsms(msisdn,txt):
    sURL1 = "http://10.101.11.164:10144/cgi-bin/sendsms?user=tester&pass=foobar&to="
    sURL2 = "&from=10144&text="
    sURL_pgon = sURL1 + msisdn + sURL2 + txt
    resp = rs.get(sURL_pgon)
    print(resp)

def smscheck():
    smsinbox = "SELECT * from [dbo].[USDLOG_ROCAPP] WHERE INSERT_TIME BETWEEN '" + qryst + "' AND '" + qryend + "';"
    dfsms = pd.read_sql(smsinbox, conx)
    return dfsms


def main():
    df = smscheck()
    if df.shape[0] != 0:
        for i in range(len(df)):
            print(df.iloc[i,1])
    else:
        print('no sms')