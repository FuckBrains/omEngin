from datetime import *
from dateutil.relativedelta import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.parser import *


def PTM(d,fmt):
    if isinstance(d,str):
        tm = parse(d)
        #print(type(tm),tm)
        str_d = tm.strftime(fmt)
        print(str_d)
    else:
        d1 = d
        d = str(d1)
        tm = parse(d1)
        #print(type(tm),tm)
        str_d = tm.strftime(fmt)
        print(str_d)

D1 = "2020-11-07 04:05:00"
D2 = "07-11-2020 05:12:00"
PTM(D1,"%Y-%m-%d %H:%M:%S")
