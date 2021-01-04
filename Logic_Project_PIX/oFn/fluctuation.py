import os, cx_Oracle
import time as ti
import requests
import numpy as np
import pandas as pd
from fn import *
from oDT import *
from datetime import *

livedb = os.getcwd () + "\\robi_live.csv"
db = os.getcwd () + "\\OMDB.csv"
semcol = os.getcwd () + "\\semcols.txt"
CAT = os.getcwd () + "\\CATdef.txt"

nw = datetime.now()
td = nw.strftime("%Y_%b_%d")
mylog = os.getcwd() + "\\log"
todaylog = os.getcwd() + "\\log\\" + td
print(todaylog)

try:
    os.makedirs(mylog)
    os.makedirs(todaylog)
    print("folder created successfully")
except:
    try:
        os.makedirs(todaylog)
    except:
        print(" todayslog folder exits + ")

n = datetime.now ()
td = n.today()
#print(str(td) + "00:00:00")
tm = n.strftime("%H:%M") + " on " + n.strftime ("%m-%d-%Y")


def w2t(text):
    nx = datetime.now ()
    file1 = os.getcwd() + "\\" + nx.strftime("%m%d%H%M%S") + ".txt"
    file2 = os.getcwd() + "\\dump\\" + nx.strftime("%m%d%H%M%S") + ".txt"
    try:
        try:
            f = open(file2, 'a+')
        except:
            f = open(file1, 'a+')
        f.write("\n")
        f.write(text)
        f.close()
    except:
        pass
    print(file)
    return ""

def tmsg(chatid,msg):
    TOK = "1176189570:AAEfPi9TIZIbnhWi4Ko6KQev2Iv7UbMw5js"
    url = "https://api.telegram.org/bot" + TOK + "/sendMessage?chat_id=" + str(chatid) + "&text=" + msg
    requests.get(url)
    return ""

def hr_minus(diff):
    x = datetime.now ()
    d = x - timedelta (hours=diff)
    str_d = d.strftime ("%m-%d-%Y %H:%M:%S")
    return str_d

def lasthr(diff = 1):
    x = datetime.now ()
    d = x - timedelta (hours=diff)
    str_d = d.strftime ("%H")
    return str_d

def timedelt(diff):
    x = datetime.now ()
    d = x + timedelta (hours=diff)
    str_d = d.strftime ("%d-%m-%Y %H:%M:%S")
    return str_d

def semqry():
    conn = cx_Oracle.connect ('SOC_READ','soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
    print (conn.version)
    agent = ['U2000 TX','Ericsson OSS','EricssonOSS','Huawei U2000 vEPC','Huawei U2020','LTE_BR1_5','MV36-PFM3-MIB','BusinessRule14','BusinessRule14_ERI_ABIP']
    cols = "SERIAL,NODE,AGENT,ALERTGROUP,X733EVENTTYPE,X733SPECIFICPROB,CLASS,GEOINFO,CUSTOMATTR3,CUSTOMATTR5,CUSTOMATTR26,TTSEQUENCE,ALARMDETAILS,EQUIPMENTKEY,SITECODE,SUMMARY,LASTOCCURRENCE,CLEARTIMESTAMP"
    #cols = "SERIAL,NODE,AGENT,SUMMARY,LASTOCCURRENCE,CLEARTIMESTAMP,EQUIPMENTKEY,SITECODE"
    q1 = "SELECT " +  cols + " FROM SEMHEDB.ALERTS_STATUS_V_FULL WHERE "
    x = n
    hr = x.strftime('%H')
    STDT = timedelt(-int(hr))
    ENDT = timedelt(1)
    q2 = "LASTOCCURRENCE BETWEEN TO_DATE('" + STDT + "','DD-MM-YYYY HH24:MI:SS') AND TO_DATE('" + ENDT + "','DD-MM-YYYY HH24:MI:SS')"
    q3 = q1 + q2
    print(q3)
    print('starts: ', datetime.now())
    df = pd.read_sql(q3, con=conn)
    print ('ends: ', datetime.now())
    print(df.shape[0])
    print(df.columns)
    try:
        df = df.rename(columns = {'SITECODE':'CUSTOMATTR15'})
    except:
        pass
    #df1 = df[df['AGENT'].isin([agent])]
    #print (df.shape[0])
    df.to_csv(os.getcwd () + "\\SEMQRY.csv", index=False)
    ti.sleep(2)
    df2 = pd.read_csv(os.getcwd () + "\\SEMQRY.csv")
    print(df2)
    return df2

def wrt2txt(contents, filename = 'excmd', flpath = None):
    print('contets', contents)
    filename =  n.strftime("%Y%m%d%H%M%S")
    if flpath == None:
        flpath = tm + '.txt'
    else:
        flpath = flpath + "\\" + filename + '.txt'
    content = "executed commands"
    if isinstance(contents, list):
        for i in range(len(contents)):
            content = content + chr(10) + contents[i]
    else:
        content = contents
    try:
        f = open(flpath, 'w+')
        f.write(content)
        f.close()
        print('print from wrt2txt, *success*', flpath, chr(10))
    except:
        lastslash = flpath.rfind('\\')
        flname = flpath[-lastslash :len(flpath)-4]
        print(flname)
        os.system("taskkill /F /FI '"+ flname + "' /T")
        try:
            f = open(flpath, 'w+')
            f.write(content)
            f.close()
            print('print from wrt2txt, *success*', flpath, chr(10))
        except:
            print('def wrt2txt *failed* ', flpath, chr(10))


def filter_p(df,reflst,oncolumn):
    i = 0
    dfx = pd.DataFrame([])
    rw = 0
    for k in reflst:
        i = i + 1
        ndf = df[df[oncolumn].str.contains(k)]
        rw = ndf.shape[0]
        if rw >= 2:
            if i == 1:
                dfx = ndf
            else:
                dfy = pd.concat([dfx,ndf])
                dfx = dfy
                dfy = pd.DataFrame([])
    else:
        return dfx

def text2list(pth):
    f = open (pth, 'r+')
    ls = []
    for i in f.readlines ():
        ls.append (i.replace ('\n', ''))
    return ls


def text2dic(pth):
    f = open (pth, 'r+')
    dc = {}
    for i in f.readlines():
        a1 = i.replace ('\n', '')
        a2 = a1.split (':')
        dc[a2[0]] = a2[1]
    return dc


def getkey(my_dict, ky):
    if ky is not None:
        for key, value in my_dict.items ():
            if key in str (ky):
                return value
        else:
            return "other"


DRCAT = lambda x: 'H2' if (x < 60) else ('H12')

TS = lambda x: '2G' if ('2G SITE DOWN' in x) \
    else ('2G' if ('2G CELL DOWN' in x) \
    else ('3G' if ('3G SITE DOWN' in x) \
    else ('3G' if ('3G CELL DOWN' in x) \
    else ('4G' if ('4G SITE DOWN' in x) \
    else ('4G' if ('4G CELL DOWN' in x) \
    else ('2G' if ('OML' in x) \
    else "other"))))))


def extrafeat(xdf, tmdelta = 0):
    xdf = xdf.rename (columns=str.upper)
    df = xdf.assign (DURCAT='0')
    df = df.assign (LO='0')
    df = df.assign (CDLO='0')
    df = df.assign (CDLOTECH='0')
    df['DURCAT'] = df.apply (lambda x: DRCAT (x.DUR), axis=1)
    df['LO'] = df.apply (lambda x: pd.to_datetime (x['LASTOCCURRENCE'], errors='coerce', cache=True).strftime("%d%m%y%H%M"), axis=1)
    df['CDLO'] = df['CUSTOMATTR15'].str.cat (df['LO'])
    df['CDLOTECH'] = df['CDLO'].str.cat (df['CATX'])
    print('extrafeat')
    return df

def prob(df):
    df.to_csv (os.getcwd () + "\\FINAL15.csv", index=False)
    xdf = pd.read_csv(os.getcwd () + "\\FINAL15.csv")
    ndf = countifs(xdf, xdf['CUSTOMATTR15'], xdf['CUSTOMATTR15'], xdf['DURCAT'], xdf['DURCAT'])
    odf = countifs(ndf, xdf['EQUIPMENTKEY'], xdf['EQUIPMENTKEY'], xdf['DURCAT'], xdf['DURCAT'])
    
    print(odf.shape[0])
    try:
        fdf.to_csv (os.getcwd () + "\\FINAL12.csv", index=False)
    except:
        fdf.to_csv (os.getcwd () + "\\FINAL13.csv", index=False)
    print('final', fdf.shape[0])
    print(fdf.columns)
    pvt = fdf.pivot_table(index=['CUSTOMATTR15','CAT'], columns='DURCAT', values='cnt', aggfunc='sum').reset_index()
    pvt.to_csv(os.getcwd () + "\\pvt.csv", index = False)
    pvt = pd.read_csv(os.getcwd () + "\\pvt.csv")
    ndf = pvt[(pvt['H2'] > 1) & (pvt['H12'] > 10)]
    return ndf

def append_dic_value(dict_obj, key, value):
    if key in dict_obj:
        if not isinstance(dict_obj[key], list):
            dict_obj[key] = [dict_obj[key]]
        dict_obj[key].append(value)
    else:
        dict_obj[key] = value

def dic_by_key(dc, ky):
    hp = ky + " : "
    for key in dc:
        if key == ky:
            ls = dc[key]
            if isinstance(ls, list):
                for i in range(len(ls)):
                    if ls[i] not in hp:
                        hp = hp + chr(10) + ls[i]
                    else:
                        pass
                else:
                    if len(hp) < 8:
                        return "3G - 0"
                    else:
                        return chr(10) + hp
                    exit()
            elif ls is None or ls == '':
                return hp + " 0" + chr(10)
                exit()
            else:
                try:
                    hp = chr(10) + hp + chr(10) + ls
                    return hp
                    exit()
                except:
                    return hp + " 0" + chr(10)
                    exit()


def catmap_mod(df):
    print("strart operation..............")
    dfdb1 = pd.read_csv (db)
    dfdb = dfdb1[['Code', 'Zone']]
    df0 = df.rename (columns=str.upper)
    ls = text2list (semcol)
    df1 = df0[ls]
    dc = text2dic (CAT)
    df1 = df1.assign (CAT='0')
    df1 = df1.assign (CATX='0')
    df1 = df1.assign (Code='0')
    df1['CAT'] = df1.apply (lambda x: getkey (dc, x.SUMMARY), axis=1)
    df1['CATX'] = df1.apply (lambda x: TS (x.SUMMARY), axis=1)
    df1['Code'] = df1.apply (lambda x: x.CUSTOMATTR15[0:5], axis=1)
    df2 = df1.merge (dfdb, on='Code')
    df3 = pd.DataFrame([])
    try:
        df3 = DateDiff(df2, "DUR", "LASTOCCURRENCE")
    except:
        df3 = datediff_ondf(df2, "DUR", 'LASTOCCURRENCE')
    print(df3)
    xdf = filter_p(df3, ['2G', '3G', '4G'], 'CATX')
    xdf.to_csv(os.getcwd () + "\\FINAL11.csv", index=False)
    df4 = extrafeat(xdf)
    return df4

def sort_rvmdup(df):
    print('sort_rvmdup')
    df1 = df.sort_values(by=['CAT','CDLO'], ascending=True)
    df1 = df1.drop_duplicates(subset=['CDLOTECH'], inplace=False, ignore_index=True)
    df1.to_csv (os.getcwd () + "\\FINAL13.csv", index=False)
    df1 = pd.read_csv(os.getcwd () + "\\FINAL13.csv")
    #df2 = df1.groupby(['DURCAT','EQUIPMENTKEY','CAT'])['CUSTOMATTR15'].count()
    return df1


def nonechk(tech, x):
    try:
        y = len(x)
        print(y)
        return x
    except:
        return chr(10) + tech + ': NA'

def final_mod(xdf):
    shp = xdf.shape[0]
    print('fmod start- ', shp)
    df1 = xdf.sort_values(by=['LASTOCCURRENCE'], ascending=True)
    df1 = df1.reset_index()
    print('fmod after reset index at line 266 - ', df1.shape[0])
    df1 = df1.assign(hr = '0')
    df1['hr'] = df1.apply (lambda x: pd.to_datetime (x['LASTOCCURRENCE'], errors='coerce', cache=True).strftime("%H"), axis=1)
    hr = lasthr(1)
    dfh = df1[df1['hr'].isin([hr])]
    print('pick for last hour line 171 and found shape ', dfh.shape[0], ' checking hour ', hr)
    dff = dfh.reset_index()
    print('save file asdff after reset index at line 273- found row length', dff.shape[0])
    df = dff[['EQUIPMENTKEY','CUSTOMATTR15','CAT','CATX']]  ##-------------------df1 = all------------df = this hour
    df.to_csv(os.getcwd () + "\\OKOK.csv", index = False)
    print("\n \n")
    site = {}
    cell = {}
    A = {}
    B = {}
    df = df.astype(str)   #this hour
    df1 =  df1.astype(str)
    hpp = []
    qn = 0
    for i in range(len(df)):
        cat = df.loc[i,'CAT']
        qn = qn + 1
        if int(cat) == 2 or int(cat) == 3 or int(cat) == 4:
            ctx = df.loc[i,'CATX']
            code = df.loc[i, 'CUSTOMATTR15']
            this_hr = countifs(df, df['CUSTOMATTR15'], code, df['CAT'], str(cat))
            all_hr = countifs(df1, df1['CUSTOMATTR15'], code, df1['CAT'], str(cat))
            rest_hr = int(all_hr) - int(this_hr)
            #print(this_hr, all_hr, rest_hr)
            heap1 = code + "-" + str(ctx) + " running for Hour: " + str(hr) + " ~count for thishour-"  + str(this_hr) + "- all hour-" + str(all_hr)
            #print(heap1)
            hpp.append(heap1)
            if rest_hr >= 10:
                st = code + " - " + str(all_hr)
                #print(ctx , st)
                append_dic_value(site, ctx, st)
            else:
                #print('~~~~~~~', ctx, code)
                pass
        elif int(cat) == 22 or int(cat) == 33 or int(cat) == 44:
            ctx = df.loc[i,'CATX'] + " Cell"
            code = df.loc[i, 'EQUIPMENTKEY']
            this_hr = countifs(df, df['EQUIPMENTKEY'], code, df['CAT'], str(cat))
            all_hr = countifs(df1, df1['EQUIPMENTKEY'], code, df1['CAT'], str(cat))
            heap1 = code + " (" + str(ctx) + ") - last Hour fluc: " + str(this_hr) + ", fluc from 00:" + str(all_hr)
            hpp.append(heap1)
            rest_hr = int(all_hr) - int(this_hr)
            #print(this_hr, all_hr, rest_hr)
            if rest_hr >= 10:
                st = code + " - " + str(all_hr)
                #print(ctx , st)
                append_dic_value(cell, ctx, st)
            else:
                #print('~~~~~~~', ctx ,code)
                pass
        else:
            print('else')
    wrt2txt(hpp, "fluc_process", todaylog)       
    G2 = nonechk("2G ", dic_by_key(site, "2G"))
    G3 =  nonechk("3G ", dic_by_key(site, "3G"))
    G4 =  nonechk("4G ",dic_by_key(site, "4G"))
    G22 = nonechk("2G Cell ", dic_by_key(cell, "2G Cell"))
    G33 = nonechk("3G Cell ", dic_by_key(cell, "3G Cell"))
    G44 = nonechk("4G Cell ", dic_by_key(site, "4G Cell"))
    sitewise = G2 + chr(10) + G3 + chr(10) + G4
    cellwise = G22 + chr(10) + G33 + chr(10) + G44
    HD1 = "Site Fluctuation Status" + chr (10) + "at " + tm + chr (10) + sitewise
    HD2 = "Cell Fluctuation Status" + chr (10) + "at " + tm + chr (10) + cellwise
    print(HD1)
    print(HD2)
    msk = '-1001199723504'
    q1 = tmsg (msk, HD1)
    q2 = tmsg (msk, HD2)
    wrt2txt(hpp)

    
def fmtmsg_techwise(ndf, name_thread_col, ls_datacol, name_catcol, cat_text):
    lss = []
    hpx = ""
    colx = ndf.columns.to_list()
    print(colx)
    df = ndf[["CUSTOMATTR15","CAT","H2","H12"]]
    for n in range(len(df)):
        cat = df.iloc[n, 1]
        if str(cat) == cat_text:
            try:
                code = df.iloc[n, 0] + ": " + str(df.iloc[n, 2]) + " | " + str(df.iloc[n, 3])
                lss.append(code)
                hpx = hpx + chr(10) + code
            except:
                pass
        else:
            pass
    print(lss)
    return hpx
        





def save2db(df):
    soc ="Driver={SQL Server};SERVER=192.168.88.121;DATABASE=SOC_Roster;UID=sa;PWD=Robi456&"
    conn = pyodbc.connect(soc)
    xx = df2sq(df, 'fluc1', conn, bycol='SERIAL')

print(os.getcwd())
#svpt = os.getcwd () + "\\SEMQRY.csv"
#df = pd.read_csv (svpt, low_memory=False)
#conn = cx_Oracle.connect ('SOC_READ','soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')
#print (conn.version)
#conn.close()
#import qrybuilt as qr
#from omsql.create_table.tbl_mssql import *

def main(df):
    #cols = ["SERIAL,NODE,AGENT,SUMMARY,LASTOCCURRENCE,CLEARTIMESTAMP,EQUIPMENTKEY,SITECODE"]
    df = df.astype (str)
    df1 = catmap_mod(df)
    df1 = df1.astype (str)
    #try:
        #CreateTable_MSSQL(df1, 'fluc1', con)
        #dfqry = qr.qbuilt(df1,'fluc1',['SERIAL'])
        #dfqry.to_csv(os.getcwd () + "\\qry.csv", index=False)
    #except:
        #pass
    df2 = sort_rvmdup(df1)
    df4 = final_mod(df2)
    #con = qr.mssql_121()
    #cr= con.cursor()
    #for i in range(len(dfqry)):
        #a1 = dfqry.iloc[i,1]
        #a2 = dfqry.iloc[i,2]
        #try:
            #cr.execute(a1)
        #except:
            #cr.execute(a2)
    #else:
        #con.commit()
    
    #con = mssql_121()
    #print(updf.shape[0])
    #updf = updf.reset_index()
    #updf.to_csv(os.getcwd () + "\\updf.csv", index = False)
    #udf = pd.read_csv(os.getcwd () + "\\updf.csv")
    #ud = udf[['NODE','AGENT','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','EQUIPMENTKEY','CUSTOMATTR15','CAT','CATX','CODE','ZONE','DUR','DURCAT','LO','CDLO','CDLOTECH','SERIAL']]
    #sq.create_table(ud, 'fromfluc2', con)
    #try:
        #sq.update_table(ud, 'SOC_Roster', 'fromfluc2', con, ['SERIAL'])
    #except:
        #sq.upload_bulkdata(ud,'fromfluc2', con, 'SOC_Roster')
        

df = semqry()
y = main(df)
    
#xdf = pd.read_csv(os.getcwd () + "\\FINAL13.csv")
#print(xdf)
#df4 = final_mod(xdf)



#ls = ['NODE','AGENT','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','EQUIPMENTKEY','CUSTOMATTR15','CAT','CATX','CODE','ZONE','DUR','DURCAT','LO','CDLO','CDLOTECH','SERIAL']
#df1 = pd.read_csv(os.getcwd () + "\\updf.csv")
#dfx = df1[ls]
#conn = sq.mssql_121()
#sq.df_to_sql(dfx, 'SOC_Roster', 'fromfluc2', conn, bycolumn=['CDLOTECH'])
#a.to_csv(os.getcwd () + "\\aa.csv")
#df = pd.read_sql("select * from fromfluc2", con = conn)
#print(df)


