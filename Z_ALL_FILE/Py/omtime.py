from datetime import *
from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *
import os
import pandas as pd
import numpy as np

def datetime_re_format(ls, fmt='%Y/%m/%d %H:%M'):
    #serialize and convert using dateutil.parser and datetime.strftime
    if ls is not None and isinstance(ls, list):
        lss = []
        for i in range(len(ls)):
            try:
                dt = parse(str(ls[i])).strftime(fmt)
                lss.append(dt)
            except:
                lss.append(ls[i])
        else:
            return lss

#diffdate = lambda T1, T2 : (datetime.strptime(T2, "%d/%m/%Y %H:%M") - datetime.strptime(T1, "%d/%m/%Y %H:%M")).total_seconds()/60
diffdate = lambda T1, T2 : datetime(T2-T1).total_seconds()/60
diff_from_now = lambda locc : (datetime.now() - datetime.strptime(locc, "%d/%m/%Y %H:%M")).total_seconds()/60

def makelist_dttm_now(ln):
    nw = datetime.now()
    st = nw.strftime("%d/%m/%Y %H:%M")
    ls = []
    for i in range(ln):
        ls.append(st)
    return ls

def formatchk(L1):
    if isinstance(L1, list):
        return L1
    elif isinstance(L1, pd.core.series.Series):
        ls = L1.to_list()
        return ls
        

def DateDif(DT1, DT2 = None):
    TM1 = formatchk(DT1)
    if DT2 is None:
        TM2 = makelist_dttm_now(len(DT1))
    else:
        TM2 = formatchk(DT2)
    try:
        TM11 = datetime_re_format(TM1)
        TM22 = datetime_re_format(TM2)
        dur = list(map (lambda LO , CL: diffdate(LO,CL) if ('1970' not in str(CL)) else diff_from_now(LO), TM11, TM22))
        return dur
    except:
        print("except")
        return []



def help_omtime():
    a1 = """#------------dataframe calclate datetime difference ---------------------\n#
# main function -> DateDif(DT1, DT2 = None) DT1 & DT2 can 'pandas series/list', return 'list'
    - if DT2 = None, it will find difference from 'now - DT1'
    - if DT3 provided but value contains '1970' then, calculate diff as 'now - DT1'
    - any datetime format can be handdled, even DT1 and DT2 format is different as using dateut
#----------- Example -------------------#
# df.assign(dur = 'x')
# df['dur'] = np.array(DateDif(df['LASTOCCURRENCE'],df['CLEARTIMESTAMP']))
# lst = DateDif(df['LASTOCCURRENCE']) 
# df['DUR'] = np.array(lst)
#--------------------------------------#"""
    print(a1)

#help_omtime()


def parse_dt(txt):
    n = datetime.now()
    yr = n.strftime("%y")
    print(yr)
    if 'TODAY' in txt:
        str_d = n.strftime("%Y-%m-%d")
        return str_d
    else:
        try:
            x = parse(txt, fuzzy=True, dayfirst = True)
            yx = x.strftime("%Y-%m-%d")
            return yx
        except:
            return 0

b1 = "you can write date by 4 format" + chr(10) + "12-sept or 12/09/20 or 12092019 or 12-09-19"
b2 = "12-sept"
print(parse_dt(b2))



#df = pd.read_csv(os.getcwd() + "\\FINAL15.csv")
#df['Diff'] = np.array(DateDif(df['LO'],df['CLR']))
#print(df[['LO','CLR','Diff']])
