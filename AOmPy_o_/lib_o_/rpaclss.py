import pandas as pd
import numpy as np
import os
from datetime import *
from collections import defaultdict

try:
    from lib_o_.o_fn import *
    from lib_o_.o_rpadef import *
    from lib_o_.o_rpafn import *
    import lib_o_.o_time as oT
except:
    from o_fn import *
    from o_rpadef import *
    from o_rpafn import *
    import o_time as oT

def inner_list_to_dic(dic):
    for key, value in dic.items(): 
        dic[key] = set(value)
    return dic

def joinls(l1,l2):
    if len(l1)!=0 and len(l2)!=0:
        lss = []
        for i in l1:
            for j in l2:
                lss.append(str(i) + '$' + str(j))
        return lss
    elif len(l1)!=0 and len(l2)==0:
        return l1
    elif len(l1)==0 and len(l2)!=0:
        return l2
    else:
        return l1
    
def colchk(df):
    mcols = ['EQUIPMENTKEY','SITECODE','SUMMARY','ALERTKEY','LASTOCCURRENCE','CLEARTIMESTAMP']
    ocols = ['RESOURCE','CUSTOMATTR15','SUMMARY','ALERTKEY','LASTOCCURRENCE','CLEARTIMESTAMP']
    df = df.rename (columns=str.upper)
    cols = df.columns.to_list()
    if cols.count('SITECODE') != 0:
        df = df.rename(columns={'SITECODE':'CUSTOMATTR15'})
    if cols.count('EQUIPMENTKEY') != 0:
        df = df.rename(columns={'EQUIPMENTKEY':'RESOURCE'})
    for i in ocols:
        if ocols.count(i) == 0:
            print('must have column needs in table: but missing !',chr(10),mcols,chr(10),'exiting .....')
            exit(0)
    else:
        sx = chrstream()
        omnm(sx)
        print(chr(10))
        return df

def nested_dic_add(dc, k, v):
    #map based on child dictionary and add new key value/append by key matching on clild
    if len(dc)>0:
        ln = len(list(dc))
        for i in dc:
            if k in list(dc[i]):
                dc[i][k] = v if not list(dc[i]) else dc[i].get(k, []) + [v]
                return dc
        else:
            if isinstance(v, list):
                dc[ln+1] = {k:v}
            else:
                v1 = [v]
                dc[ln+1] = {k:v1}
            return dc
    else:
        if isinstance(v, list):
            dc[1] = {k:v}
        else:
            v1 = [v]
            dc[1] = {k:v1}
        return dc    

def nested_dic_1(cond, ky, vl):
        cond.setdefault(ky, []).append(vl)
        
def nested_dic_2 (cond, ky, vl):
    if isinstance(vl,list):
        cond[ky] = vl if not list(cond) else cond.get(ky, []) + vl
    else:
        vlx = [vl]
        cond[ky] = vlx if not list(cond) else cond.get(ky, []) + vlx
    return cond
    

class RPA:
    def __init__(self, mdf):
        self.odb = omdb()
        self.df0 = colchk(mdf)
        self.df1 = mpdata(self.df0, self.odb)
        self.df = self.df1
        self.pickcols = "CDLO"
        self.msgthread = defaultdict(dict)
        self.mth = []
        self.cond = defaultdict(dict)
        self.dic = defaultdict(list)
        self.lsky = []
        self.lsvl = []
        self.lsx = []
        print(self.df.columns)
    def regionwise(self, Name=[]):
        self.msgthread = list(zn_dic())
    def techwise(self):
        self.msgthread = {'CAT': {'2G':"","3G":"","4G":""}}
    def msgitems(self, lscols):
        fault = 0
        nm = ""
        col = self.df.columns.to_list()
        for i in lscols:
            if col.count(i)==0:
                fault = 1
                nm = i
        else:
            if fault == 0:
                self.df = self.df.assign(pk = self.df[lscols].apply(lambda x: '- '.join(x.values.astype(str)), axis=1))
                self.pickcols = "pk"
            else:
                print("!!!! column name not found: ", nm, chr(10), self.df.columns)
    def csvmap(self, csvpath, match_col_name, column_to_pick = False):
        try:
            try:
                dff = pd.read_csv(csvpath)
            except:
                dff = csvpath
            if column_to_pick:
                dff = dff[column_to_pick]
            ndf = self.df
            self.df = ndf.merge (dff, on=match_col_name)
            print(self.df.columns)
        except:
            print('csvpath not found')
    def pnt(self):
        print(self.lsky)
        print(self.lsvl)
        print(self.cond)
        print(self.dic)
    def getdf(self, current=True):
        if current==False:
            return self.df0
        else:
            return self.df
    def sample(self):
        print(self.df.head(5))
    def summary(self):
        print("COLUMN:", self.df.columns, chr(10))
        print("Current Row: ", self.df.shape[0],'--',"Row Main:", self.df1.shape[0], chr(10))
        print("filtering conditions : ", dict((k, v) for k, v in self.cond.items()))
        print("msgformat by pickcols: ", 'CUSTOMATTR15','LASTOCCURRENCE')
        print("msgthread: ", self.msgthread)
    def rwise(self, whichzn=False):
        if whichzn == False:
            whichzn = "ALL"
        if len(self.cond) == 1:
            colsMain = list(self.cond[1])
            rval = parsing(self.df, whichzn,[],colsMain[0], self.cond[1].get(colsMain[0]), False, False)
        elif len(self.cond) == 2:
            colsMain = list(self.cond[1])
            cl = list(self.cond[2])
            rval = parsing(self.df, whichzn, [] ,colsMain[0], self.cond[1].get(colsMain[0]), cl[0], 
                           self.cond[2].get(cl[0]))
            print(rval)
            return rval
        else:
            print('under development')
    def regionwise_count(self, list_cat):
        rv = zonewise_count(self.df1, list_cat)
        print('----------------------------------------')
        return rv
    def timecal(self, start_time_colname, end_time_colname=False):
        self.df = oT.dfdiff(self.df, start_time_colname, end_time_colname)
        print("new column name is 'DUR'")
    def timefmt(self, colname, fmt="%Y/%m/%d %H:%M:%S"):
        if len(fmt) <= 9:
            self.df[colname] = self.df.apply(lambda x: oT.sec_to_dur(x[colname]), axis = 1)
            print(self.df[colname])
        else:
            self.df[colname] = oT.datetime_convert_format(self.df, colname, fmt)
            print(self.df[colname])
    def add_cond(self, k, v):
        self.lsky.append(k)
        self.lsvl.append(v)
        xk = joinls(self.lsx,v)
        self.lsx = xk
        ndc = nested_dic_add(self.cond,k,v)
        self.cond = ndc.copy()
        ndc2 = nested_dic_2(self.dic,k,v)
        self.dic = ndc2
    def apply_cond(self, FilterOn, condition = "remove blank", operation = 'remove', newcol = False):
        if type(FilterOn) is list:
            if newcol == False:
                print("pre row: " , self.df.shape[0])
                if operation == 'remove':
                    if (condition == "" or condition == "remove blank" or condition == "NA"):
                        self.df = self.df.dropna(subset=FilterOn)
                        self.df = self.df.reset_index()
                        print("post row: " , self.df.shape[0])
                    else:
                        print('deployment wip')
                else:
                    print('nothing performed')
        else:
            print('provide col names inside list')
    def rpagen(self):
        hpf = chr(10)
        ndf = self.df
        print(ndf.columns)
        for i in range(len(self.lsx)):
            spl = self.lsx[i].split('$')
            if len(self.lsky) == len(spl):
                xdf = self.df
                for n in range(len(spl)):
                    k = self.lsky
                    v = spl[n]
                    xdf = xdf[xdf[k[n]].isin([v])]
                else:
                    x11 = " | ".join(spl) + ": " + str(xdf.shape[0])
                    if xdf.shape[0] != 0:
                        hpf = hpf + chr(10) + chr(10) + x11 + chr(10) + xdf[self.pickcols].str.cat(sep=chr(10))
        print(hpf, chr(10))
    def pntcond(self):
        L = list(self.cond.values())
        Lx = []
        if (len(L[0]))<=1:
            L1 = [item for sublist in L[0] for item in sublist]
            Lx.append(L1)
        else:
            Lx.append(L1)


