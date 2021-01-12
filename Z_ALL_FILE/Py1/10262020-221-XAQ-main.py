import pandas as pd
import pyodbc
from datetime import date
from datetime import datetime
from datetime import timedelta
import requests as rs
import sqltdb as sqdb
import sitecount as st
import omfn as fn
import urllib3
import urllib.parse


tday = date.today()
tmdlta = datetime.now() + timedelta(minutes=1)
tmnw = datetime.now() - timedelta(minutes=1)
qryst = tmnw.strftime('%Y-%m-%d %H:%M:%S')
qryend = tmdlta.strftime('%Y-%m-%d %H:%M:%S')


def handdler(ussd,msg,msisdn):
    nw = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rval = ""
    if msg !="":
        ms = msisdn[-10:len(msisdn)]
        ms4sms = msisdn[-11:len(msisdn)]
        code = fn.sitecode_pick(msg)
        if "ALL" in msg  or '2G' in msg or  "3G" in msg or "SC" in msg or  "4G" in msg:
            xx = st.siteinfo(msg)
            print(nw, xx)
            yy = st.sms(ms4sms,xx)
            rval = "S"
        elif "PGSTART" in msg and code != 'NA':
            xx = st.roc(ussd,code,ms,'PGSTART')
            print(nw,xx)
            if 'PGON_DONE' in xx:
                rval = "S"
            else:
                rval = "F"
        elif "PGSTOP" in msg and code != 'NA':
            xx = st.roc(ussd,code,ms,'PGSTOP')
            print(nw,xx)
            if 'PGOFF_DONE' in xx:
                rval = "S"
            else:
                rval = "F"
        else:
            rval = "Not Related Query"
    return rval

def main():
    nww = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df = st.smscheck()
    if df.shape[0] != 0:
        for i in range(len(df)):
            msg1 = df.loc[i,"MESSAGE"]
            if isinstance(msg1, str):
                ussd = df.loc[i,"USDLogId"]
                msg = msg1.upper()
                msisdn = df.loc[i,"DESTADDR"]
                sqret = sqdb.queryussd(ussd)
                if sqret == 0:
                    st.general_qry()
                    rval = handdler(ussd,msg,msisdn)
                    st.general_qry()
                    if rval == 'S':
                        rv2 = sqdb.insertussd(ussd)
                        if rv2 == "S":
                            print('Cycle Complete for::::: ', nww, ussd, msg, msisdn)
                        else:
                            print("Cycle failed:::", nww, ussd, msg, msisdn)
                    else:
                        print(rval)
                else:
                    print('already served::', nww, ussd, msg, msisdn)
    else:
        print('no sms')
    return "done at " + nww