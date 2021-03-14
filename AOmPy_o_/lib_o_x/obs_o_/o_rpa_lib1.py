import pandas as pd
import numpy as np
import os
from datetime import *
from fn import *

def o_print(my_dict):
    for key in my_dict.items():
        x = my_dict.get(key)

def getvalue(my_dict, ky):
    if ky is not None:
        for key, value in my_dict.items ():
            if key in str (ky):
                return value
        else:
            return 0

TS = lambda x : '2G' if ('2G SITE DOWN' in x) \
                else ('3G' if ('3G SITE DOWN' in x) \
                else ('4G' if ('4G SITE DOWN' in x) \
                else ('MF' if ('MAIN' in x) \
                else ('DL' if ('VOLTAGE' in x) \
                else ('TM' if ('TEMPERATURE' in x) \
                else ('SM' if ('SMOKE' in x) \
                else ('GN' if ('GEN' in x) \
                else ('GN' if ('GENSET' in x) \
                else ('TH' if ('THEFT' in x) \
                else ('C2G' if ('2G CELL DOWN' in x) \
                else ('C3G' if ('3G CELL DOWN' in x) \
                else ('C4G' if ('4G CELL DOWN' in x) \
                else ('DOOR' if ('DOOR' in x) \
                else "NA")))))))))))))

dfd = pd.read_csv (os.getcwd() + "\\OMDB.csv")
lss = dfd['sCode'].to_list()

def codecorr(code,akey):
    cd = code
    if 'UNKNOW' in code:
        for i in range(len(lss)):
            vl = akey.find(lss[i])
            if vl > 0 and vl is not None:
                cd = akey[vl:vl+7]
                break
        else:
            return cd
    else:
        return cd

def msgprep_head_znwise(hd = "Periodic Notification"):
    nw = datetime.now()
    dt = nw.strftime("%d-%m-%Y")
    tm = nw.strftime("%H:%M")
    a1 = hd + " at " + tm + " on " + dt
    return a1

def catmap_mod(df,dfdb,dfp1p2):
    df0 = df.rename (columns=str.upper)
    ls = ['NODE','RESOURCE','CUSTOMATTR15','SUMMARY','ALERTKEY','LASTOCCURRENCE']
    df1 = df0[ls]
    df1 = df1.assign(CAT = df1.apply (lambda x: TS (x.SUMMARY), axis=1))
    df1 = df1.assign(CODE = df1.apply (lambda x: codecorr(x.CUSTOMATTR15, x.ALERTKEY), axis=1))
    df2 = df1.assign(sCode = df1.apply (lambda x: x.CODE[0:5] if (x.CODE is not None) else "XXXXXXXX", axis=1))
    df3 = df2.merge (dfdb, on='sCode')
    df3['CODECAT'] = df3['CUSTOMATTR15'].str.cat(df3['CAT'])
    df3['ZNCAT'] = df3['sZone'].str.cat(df3['CAT'])
    df3 = df3.assign(CDLO = df3.apply (lambda x: x['CUSTOMATTR15'] + ": " + x['LASTOCCURRENCE'], axis=1))
    df4 = df3.merge (dfp1p2, on='CUSTOMATTR15')
    print(df4.columns)
    return df4

def rmvdup(df,lscol=[]):
    df1 = df.drop_duplicates(subset=lscol, inplace=False, ignore_index=True)
    return df1
     
def zonewise_parse(df1, whichzn, pickcols=[], colsMain="CAT", colsMain_val=['DL'], cols2=False, ls_val_cols2=False):
    heap = ""
    if df1.shape[0] != 0:
        if len(pickcols) is not None:
            for i in range(len(pickcols)):
                df1 = df1.assign(COLO1 = "0")
                df1['CDLO1'] = df1.apply (lambda x: x['CDLO'] + " - " + x[pickcols[i]], axis=1)
                df1['CDLO'] = df1['CDLO1']
                df1 = df1.drop(['CDLO1'], axis=1)
        if df1.shape[0] != 0:
            for i in range(len(colsMain_val)):
                hp1 = ""
                cri_1 = colsMain_val[i]
                df2 = df1[df1[colsMain].isin([cri_1])]
                if df2.shape[0] !=0 and ls_val_cols2 != False:
                    for j in range(len(ls_val_cols2)):
                        cri_2 = ls_val_cols2[j]
                        dff = df2[df2[cols2].isin([cri_2])]
                        if dff.shape[0] != 0:
                            dfx = dff.reset_index()
                            hp = cri_2 + "| " + str(dff.shape[0]) + chr(10) + dfx['CDLO'].str.cat(sep=chr(10))
                            if hp1 == "":
                                hp1 = hp
                            else:
                                hp1 = hp1 + chr(10) + chr(10) + hp
                    else:
                        heap = heap + chr(10) + chr(10) + cri_1 + " || Count: " + str(df2.shape[0]) + chr(10) + chr(10) + hp1
                else:
                    if df2.shape[0] !=0:
                        df2 = df2.reset_index()
                        hp = cri_1 + ": " + str(df2.shape[0]) + chr(10) + df2['CDLO'].str.cat(sep=chr(10))
                        if heap == "" or heap== chr(10):
                            heap = hp
                        else:
                            heap = heap + chr(10) + chr(10) + hp
                    else:
                        heap = heap + chr(10) + cri_1 + ": " + "NA"
            else:
                finalout = "Region: " + whichzn + chr(10) + heap
                return finalout
                
def parsing(df, whichzn, pickcols=[], colsMain="CAT", colsMain_val=['DL'], cols2=False, ls_val_cols2=False):
    zn = {'DHK_M':'','DHK_N':'','DHK_O':'','DHK_S':'','CTG_M':'','CTG_N':'','CTG_S':'','COM':'','NOA':'',
          'BAR':'','KHL':'','KUS':'','MYM':'','RAJ':'','RANG':'','SYL':''}
    cnt = 0
    rval = ""
    if whichzn =="ALL":
        for ky in zn.keys():
            cnt = cnt + 1
            whichzn = ky
            df1 = df[df['sZone'].isin([ky])]
            if ls_val_cols2 != False:
                zn[ky] = zonewise_parse(df1,whichzn,pickcols,colsMain, colsMain_val, cols2, ls_val_cols2)
                print(zn.get(ky))
                print("############################",chr(10))
            else:
                zn[ky] = zonewise_parse(df1,whichzn,pickcols,colsMain, colsMain_val)
                print(zn.get(ky))
                print("############################",chr(10))
        else:
            return zn
    elif whichzn =="":
        if ls_val_cols2 != False:
            rval = zonewise_parse(df,whichzn,pickcols,colsMain, colsMain_val, cols2, ls_val_cols2)
        else:
            rval = zonewise_parse(df,whichzn,pickcols,colsMain, colsMain_val)
        return rval
    elif whichzn !="NA" and whichzn!="ALL" and whichzn !="":
        df1 = df[df['sZone'].isin([whichzn])]
        if ls_val_cols2 != False:
            rval = zonewise_parse(df1,whichzn,pickcols,colsMain, colsMain_val, cols2, ls_val_cols2)
        else:
            rval = zonewise_parse(df1,whichzn,pickcols,colsMain, colsMain_val)
        return rval
                
            
            
            
#df2 = df1[df1[cols1].str.contains(cols1_val) & df0['sZone'].str.contains(znst)]
#df1 = df.assign(LOCD = xdf.apply(' - '.join, axis=1))
#pickcols.insert(0, 'sZone')
#pickcols.insert(0, 'CAT')
#ndf = df0[df0[cols1].str.contains(cols1_val) & df0['sZone'].str.contains(znst)]
        
    
#sclick.csv must have Required column : 'NODE','RESOURCE','CUSTOMATTR15','SUMMARY','ALERTKEY','LASTOCCURRENCE'
df = pd.read_csv(os.getcwd() + "\\sclick.csv")  # data source, 
dfdb = pd.read_csv (os.getcwd() + "\\OMDB.csv") #fixed data in same folder
dfp1p2 = pd.read_csv (os.getcwd() + "\\site_p1p2.csv")
odf = catmap_mod(df,dfdb,dfp1p2) # function is for processing data
#ST = zonewise_count(xx, ['2G','3G','4G'])  #2G,3G,4G derived from lambda function "TS". check abov
#result = parsing(odf,whichzn="ALL",pickcols=['Name'],colsMain="CAT", colsMain_val=['DL','MF'], cols2="ATRB", ls_val_cols2=['eco','Ulka'])
#result = parsing(odf,whichzn="COM",pickcols=['Name'],colsMain="CAT", colsMain_val=['DL','MF'], cols2="ATRB", ls_val_cols2=['eco','Ulka'])
result = parsing(odf,whichzn="",pickcols=['Name'],colsMain="CAT", colsMain_val=['2G','3G','4G'])
try:
    print(result)
except:
    o_print(result)


