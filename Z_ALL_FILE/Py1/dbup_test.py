import pandas as pd
import numpy as np
import os
import func.fndatetime as fdt
import func.fnlook as flk
import func.fnstr as fst
import db.db as sq
from datetime import *
from dateutil.relativedelta import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.parser import *
import db.omdb as od

pt1 = os.getcwd() + "\\refdb\\S30.csv"
pt2 = os.getcwd() + "\\refdb\\S1800_200.csv"
df1 = pd.read_csv(pt1)
df0 = pd.read_csv(pt2)

def con_sec(sec):
    time = float(sec)
    day = time // (24 * 3600)
    time = time % (24 * 3600)
    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time
    return "%d:%d:%d" % (hour + 24*day, minutes, seconds)

def datediff(unit,datetime1,datetime2):
    d1 = ""
    d2 = ""
    try:
        if isinstance(datetime1, str):
            d1 = parse(datetime1)
        elif isinstance(datetime1, datetime):
            d1 = datetime1
        if isinstance(datetime2, str):
            d2 = parse(datetime2)
        elif isinstance(datetime2, datetime):
            d2 = datetime2
        if unit == 'n':
            return round(abs((d1 - d2)).total_seconds()/60,3)
        elif unit == 'h':
            return round(abs((d1 - d2)).total_seconds()/3600,3)
        elif unit == 's':
            return round(abs((d1 - d2)).total_seconds(),3)
        elif unit == '':
            x = con_sec(abs(d1 - d2).total_seconds())
            return x
    except:
        return "NA"

def get_date(d1):
    tm = parse(d1)
    dt = tm.date()
    return str(dt)

def PTM(d,fmt=False):
    if fmt == False:
        fmt = "%Y-%m-%d %H:%M:%S"
    if isinstance(d,str):
        tm = parse(d)
        str_d = tm.strftime(fmt)
        return str_d
    elif isinstance(d,datetime):
        d1 = d
        d = str(d1)
        tm = parse(d1)
        str_d = tm.strftime(fmt)
        return str_d
    else:
        return 0

def dateformat(d1):
    if isinstance(d1, str):
        d = datetime.strptime(d1)
        return d.strftime("%Y-%d-%m %H:%M:%S")
    if isinstance(d1, datetime):
        d = d1.strftime("%Y-%d-%m %H:%M:%S")
        return d

conn = od.MySql_3('127.0.0.1','root','admin','om1')
cr = conn.cursor()
x = od.prep_query("tm1")

def lp():
    for i in range(len(df0)):
        CL = df0.loc[i,"CLEARTIMESTAMP"]
        LO = PTM(df0.loc[i,"LASTOCCURRENCE"])
        CLR = PTM(df0.loc[i,"CLEARTIMESTAMP"])
        ls = []
        if '1970' not in CL:
            AG = datediff("",LO,CLR)
            DT = get_date(LO)
            st = "'" + DT + "','" + LO + "','" + CLR + "','" + AG + "','" + df0.loc[i,"CUSTOMATTR15"] + "'"
            print(st)
            x.q_insert("`TODAY`, `LO`, `CLR`, `AG`, `CODE`",st)
            ist = x.get()
            cr.execute(ist)
            conn.commit()
lp()
