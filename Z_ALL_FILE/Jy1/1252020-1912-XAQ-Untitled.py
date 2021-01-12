#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from datetime import *
from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *
from fn import *
import os


def d_diff(ls1, ls2):
    #serialize and convert using dateutil.parser and datetime.strftime
    if len(ls1) == len(ls2):
        lss = []
        for i in range(len(ls1)):
            try:
                dt1 = parse(str(ls1[i]))
                dt2 = parse(str(ls2[i]))
                if '1970' not in ls2[i]):
                    diff = (dt2 - dt1)
                    lss.append(diff)
                else:
                    diff = (datetime.now() - dt1)
                    lss.append(diff)
            except:
                lss.append('')
        else:
            return lss

diffdate = lambda T1, T2 : (datetime.strptime(T2, "%d/%m/%Y %H:%M") - datetime.strptime(T1, "%d/%m/%Y %H:%M")).total_seconds()/60
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
        dur = list(map (lambda LO , CL: diffdate(LO,CL) if ('1970' not in str(CL)) else diff_from_now(LO), TM1, TM2))
        return dur
    except:
        TM11 = datetime_re_format(TM1)
        TM22 = datetime_re_format(TM2)
        ls = d_diff(TM11, TM22)
        return ls
    

