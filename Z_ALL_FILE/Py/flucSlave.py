import pandas as pd
import numpy as np
from datetime import *
import os
from fn import *
from oDT import *
import flucsem as sem
import requests

#print(os.getcwd() + "\\B1.csv")
#df1 = pd.read_csv(os.getcwd() + "\\book1.csv")
#df = pd.read_csv(os.getcwd() + "\\B1.csv")
#nw = datetime.now()

n = datetime.now ()
td = n.today()
tm = n.strftime("%H:%M") + " on " + n.strftime ("%m-%d-%Y")

def tmsg(chatid,msg):
    TOK = "1176189570:AAEfPi9TIZIbnhWi4Ko6KQev2Iv7UbMw5js"
    url = "https://api.telegram.org/bot" + TOK + "/sendMessage?chat_id=" + str(chatid) + "&text=" + msg
    requests.get(url)
    return ""

TS1 = lambda x: '2' if ('2G SITE DOWN' in x) \
    else ('2' if ('2G CELL DOWN' in x) \
    else ('3' if ('3G SITE DOWN' in x) \
    else ('3' if ('3G CELL DOWN' in x) \
    else ('4' if ('4G SITE DOWN' in x) \
    else ('4' if ('4G CELL DOWN' in x) \
    else ('2' if ('OML' in x) \
    else "0"))))))

TS2 = lambda x: 'G2' if ('2G SITE DOWN' in x) \
    else ('C2' if ('2G CELL DOWN' in x) \
    else ('G3' if ('3G SITE DOWN' in x) \
    else ('C3' if ('3G CELL DOWN' in x) \
    else ('G4' if ('4G SITE DOWN' in x) \
    else ('C4' if ('4G CELL DOWN' in x) \
    else ('C2' if ('OML' in x) \
    else "0"))))))


DCAT = lambda x: 'H2' if (x < 90) else ('H12')

def extrafeat(df, tmdelta = 0):
    df1 = df.astype(str)
    df1 = df1.rename (columns=str.upper)
    df1 = df1[~df1['CUSTOMATTR15'].isin(['UNKNOWN'])]
    df1 = df1.assign (CT1='X')
    df1 = df1.assign (CT2='X')
    df1['CT1'] = df1.apply (lambda x: TS1 (x.SUMMARY), axis=1)
    df1['CT2'] = df1.apply (lambda x: TS2 (x.SUMMARY), axis=1)
    df1 = df1[~df1['CT1'].isin(['0'])]
    df1['CT1_1'] = df1['CUSTOMATTR15'].map(str) + '_' + df1['CT1'].map(str)
    df1['CT1_2'] = df1['CUSTOMATTR15'].map(str) + '_' + df1['CT2'].map(str)
    try:
        df2 = DateDiff(df1, "DUR", "LASTOCCURRENCE")
    except:
        df2 = datediff_ondf(df1, "DUR", 'LASTOCCURRENCE')
    df2['DCT'] = df2.apply (lambda x: DCAT(x.DUR), axis=1)
    df2['LO'] = df2.apply (lambda x: pd.to_datetime (x['LASTOCCURRENCE'], errors='coerce', cache=True).strftime("%Y%m%d%H%M"), axis=1)
    df2 = df2.astype(str)
    df2['CD_TM_CT1'] = df2['CUSTOMATTR15'].map(str) + '_' + df2['LO'].map(str) + '_' + df2['CT1'].map(str)
    df2['CD_TM_CT2'] = df2['CUSTOMATTR15'].map(str) + '_' + df2['LO'].map(str) + '_' + df2['CT2'].map(str)
    #df4 = df2.drop_duplicates(subset=['CD_TM_CT1'], inplace=False, ignore_index=True)
    #df4 = df2.reset_index()
    df2.to_csv(os.getcwd() + "\\T1.csv")
    return df2

def pivt(df):
    dfx = df.groupby(['CT1_2','DCT']).CT1_2.count().to_frame(name = 'FC').reset_index()
    pv = dfx.pivot_table(index=['CT1_2'], columns='DCT', values='FC', aggfunc='sum').reset_index()
    df = pv.drop_duplicates(subset=['CT1_2'], inplace=False, ignore_index=True)
    pv.to_csv(os.getcwd() + "\\IAMPY.csv", index = False)
    return pv
    
def pvt(df):
    pv = df.pivot_table(index=['CT1_2','DCT'], columns='DCT', values='CT1_2', aggfunc='sum').reset_index()
    print(pv)
    
def techwise(df, colnm, catby, fld0, fld1, fld2):
    hp = chr(10)
    q = 0
    for i in range(len(df)):
        n1 = df.loc[i,colnm]
        if catby in n1:
            hp = hp + chr(10) + df.loc[i,fld0] + ": " + str(df.loc[i,fld1]) + " | "  + str(df.loc[i,fld2])
    else:
        return catby + ":" + hp
    

#df = sem.semqry_dummy()
#print(df)
#fdf = extrafeat(df)
#ndf = pivt(fdf)
def flucslave(df0):
    lscol = ['SERIAL','NODE','EQUIPMENTKEY','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP']
    df = df0[lscol]
    df2 = datediff_ondf(df, "DUR", 'LASTOCCURRENCE')
    df2['DCT'] = df2.apply (lambda x: DCAT(x.DUR), axis=1)
    df2['LO'] = df2.apply (lambda x: pd.to_datetime (x['LASTOCCURRENCE'], errors='coerce', cache=True).strftime("%Y%m%d%H%M"), axis=1)
    df3 = df2[df2['SUMMARY'].str.contains('SITE DOWN') | df2['SUMMARY'].str.contains('CELL DOWN')]
    df4 = df3.assign(CT1 = np.where(df3['SUMMARY'].str.contains('SITE DOWN'), 'SITE', 'CELL'))
    df4 = df4.sort_values(by=['CT1'], ascending=False)
    df4['CT2'] = df4.apply (lambda x: TS1 (x.SUMMARY), axis=1)
    df4 = df4.astype(str)
    df4['UNQ'] = df4['CUSTOMATTR15'].map(str) + '_' + df4['LO'].map(str) + "_" + df4['CT2'].map(str)
    df5 = df4.drop_duplicates(subset=['UNQ'], inplace=False, ignore_index=True)
    df6 = df5.reset_index()
    df6 = df6[~df6['CUSTOMATTR15'].isin(['UNKNOWN'])]
    df6 = df6[~df6['CUSTOMATTR15'].isnull()]
    #df[~df['var2'].isnull()]
    df6['CT3'] = df6.apply (lambda x: TS2 (x.SUMMARY), axis=1)
    df6['CT4'] = df6['CUSTOMATTR15'].map(str) + '_' + df6['CT3'].map(str)
    df6['CT5'] = df6['CT4'].map(str) + "_" + df6['DCT'].map(str)
    ls1 = df6['CT5'].to_list()
    print(ls1)
    print(df6.columns)
    df6.to_csv(os.getcwd() + "\\Tju1.csv")
    df6 = df6.drop_duplicates(subset=['CT5'], inplace=False, ignore_index=True)
    df6 = df6.reset_index()
    st = chr(10)
    lss = []
    G2 = "2G:"
    G3 = "3G:"
    G4 = "4G:"
    cnt = 0
    for i in range(len(df6)):
        x = df6.loc[i,"CT4"]
        y1 = x + "_H12"
        y2 = x + "_H2"
        ab = df6.loc[i,"CUSTOMATTR15"] + ' - ' + df6.loc[i,"CT3"] + " - " + str(ls1.count(y2)) + "/" + str(ls1.count(y1))
        ac = df6.loc[i,"CUSTOMATTR15"] + ' - ' + str(ls1.count(y1)) + " | " + str(ls1.count(y2))
        st = st + chr(10) + ab
        if ls1.count(y1)>9 and ls1.count(y2)>0 and df6.loc[i,"CUSTOMATTR15"] is not None:
            cnt = 1
            if df6.loc[i,"CT3"] == "G2":
                G2 = G2 + chr(10) + ac
            elif df6.loc[i,"CT3"] == "G3":
                G3 = G3 + chr(10) + ac
            elif df6.loc[i,"CT3"] == "G4":
                G4 = G4 + chr(10) + ac
    else:
        if len(G2)<5:
            G2 = "2G:" + chr(10) + "NA"
        if len(G3)<5:
            G3 = "3G:" + chr(10) + "NA"
        if len(G4)<5:
            G4 = "4G:" + chr(10) + "NA"
        FG = G2 + chr(10) + chr(10) + G3 + chr(10) + chr(10) + G4 + chr(10) + chr(10)
        th1 = "Site Fluctuation Status" + chr(10) + "at " + "20:01 on 12-18-2020" + chr(10)
        th2 = "Tech: fluctuations count 00hr to last hr|only last hr count" + chr(10)
        FM = th1 + chr(10) + th2 + chr(10) + FG
        print(FM)
        msk = '-475120904'
        q1 = tmsg (msk, FM)
    

#df8 = df8.drop_duplicates(subset=['UNQ'], inplace=False, ignore_index=True)
#df8.to_csv(os.getcwd() + "\\AA5.csv", index = False)
#site = df6[df6['SUMMARY'].str.contains('SITE DOWN')]
#cell = df6[df6['SUMMARY'].str.contains('CELL DOWN')]
#dfx = df.groupby(['CT1_2','DCT']).CT1_2.count().to_frame(name = 'FC').reset_index()
#pv = dfx.pivot_table(index=['CT1_2'], columns='DCT', values='FC', aggfunc='sum').reset_index()
#df = pv.drop_duplicates(subset=['CT1_2'], inplace=False, ignore_index=True)
#pv.to_csv(os.getcwd() + "\\IAMPY.csv", index = False)









