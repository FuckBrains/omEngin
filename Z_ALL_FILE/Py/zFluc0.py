import pandas as pd
import numpy as np
import os
import requests
import lookup.lookup as look
import func.fnstr as fst
import func.fndatetime as fdt
import func.fnlook as flk
import db.db as sq
import func.fnfn as fn
import func.fn as f
import prep as pr
import func.omdtfn as odt
from datetime import *

def custom_msg_sender(chatid,msg):
    TOKEN = "961035316:AAGWIlt5GjIkBz1QI1s6WKbwVnfubmn0m6E"
    url = "https://api.telegram.org/bot" + TOKEN + "/sendMessage?chat_id=" + str(chatid) + "&text=" + msg
    requests.get(url)


def code_corr(df):
    ndf = df
    for i in range(len(ndf)):
        Eky = str(ndf.loc[i,'EQUIPMENTKEY'])
        A15 = str(ndf.loc[i,'CUSTOMATTR15'])
        if A15 == 'UNKNOWN' and len(Eky) < 15:
            if len(Eky) == 7:
                df.loc[i,'CUSTOMATTR15'] = Eky
            elif len(Eky) == 10:
                df.loc[i,'CUSTOMATTR15'] = Eky[0:7]
            elif '_' in str(Eky):
                fnd =  Eky.find('_')
                if fnd > 7:
                    df.loc[i,'CUSTOMATTR15'] = Eky[0:7]
                else:
                    try:
                        df.loc[i,'CUSTOMATTR15'] = Eky[fnd:7]
                    except:
                        df.loc[i,'CUSTOMATTR15'] = "UNKNOWN"
    return df


def smsprep_znwise(df,znname, c1,c2):
    zn = ['DHK_M','DHK_N','DHK_S','CTG_M','CTG_N','CTG_S','COM','BAR','KHL','KUS','MYM','NOA','RAJ','RANG','SYL']
    fst = ""
    for i in zn:
        st = ""
        for j in range(len(df)):
            if i == df.loc[j,znname]:
                if st == "":
                    st = i + ' XXX ' + chr(10) + df.loc[j,c1] + ': ' + str(df.loc[j,c2])
                else:
                    st = st + chr(10) + df.loc[j,c1] + ': ' + str(df.loc[j,c2])
        if len(st)> 10:
            ch10 = st.count(chr(10))
            stx = st.replace('XXX', ' || ' + str(ch10))
            if fst == "":
                fst = stx
            else:
                fst = fst + chr(10) + chr(10) + stx
            st = ""
            stx = ""
    return fst

def get_region(df):
    df4 = df
    df5 = fst.add_col_df(df4,'ShortCode')
    df5['ShortCode'] = df5.apply(lambda x : x.CUSTOMATTR15[0:5], axis = 1)
    cols = "ShortCode,Region"
    dfdb = sq.omdb(cols)
    df6 = flk.vlookup(df5,dfdb,'ShortCode','NA')
    return df6


TS = lambda x : '2G' if ('2G SITE DOWN' in x) \
                else ('3G' if ('3G SITE DOWN' in x) \
                else ('4G' if ('4G SITE DOWN' in x) \
                else ('C2G' if ('2G CELL DOWN' in x) \
                else ('C3G' if ('3G CELL DOWN' in x) \
                else ('C4G' if ('4G CELL DOWN' in x) \
                else "other")))))

TS2 = lambda x : 'H2' if (int(x) <= 119) else ('H2P' if (int(x) > 119) else "other")


def flac(df1):
    df = fst.add_col_df(df1,'cat')
    df['LASTOCCURRENCE'] = pd.to_datetime (df['LASTOCCURRENCE'], errors='coerce')
    df['cat'] = df.apply(lambda row: TS(row.SUMMARY), axis = 1)
    df1 = df[~df['cat'].isin(['other'])]
    dfx = df1.sort_values('cat')
    df3 = fst.add_col_df(dfx,'dur')
    df3['dur'] = fn.datediff(df3, 'time_cat', 'LASTOCCURRENCE')
    df3['durcat'] = df3.apply(lambda row: TS2(row.dur), axis = 1)
    df4 = fn.countifz(df3, ['EQUIPMENTKEY','cat','durcat'], 'by_cat_dur')
    #df4.to_csv(os.getcwd() + "\\xa.csv")
    dfh2 = df4[df4['durcat'].isin(['H2'])]
    dfh2P = df4[df4['durcat'].isin(['H2P'])]
    dfh2P.to_csv(os.getcwd() + "\\xa.csv")
    exit()
    df5 = df4.drop_duplicates(subset=['EQUIPMENTKEY'], keep='first')
    for i in range(len(df5)):
        res = df5.loc[i, 'EQUIPMENTKEY']
        H2 = f.countifs(dfh2, dfh2['EQUIPMENTKEY'], res)
        H2P = f.countifs(dfh2P, dfh2P['EQUIPMENTKEY'], res)
        if H2 != 0 and H2P !=0:
            print(res)

    #df5 = f.countifs(df4, df4['CUSTOMATTR15'], df4['CUSTOMATTR15'], df4['durcat'],"H12")
    #print(df5)
    #df4['fcond'] = df4.apply(lambda r: TS3(r.durcat, r.by_cat_dur), axis = 1)


df1 = pd.read_csv(os.getcwd() + "\\OMTX.csv")
x = flac(df1)

def xx():
    df4 = fst.add_col_df(df2,'DT')
    df4['DT'] = df4.apply(lambda x : pd.to_datetime(x.LASTOCCURRENCE).strftime("%d-%b-%Y"), axis = 1)
    df6 = df4.reset_index()
    ddx = code_corr(df6)
    dd = odt.day_minus(1)
    df7 = ddx[ddx['DT'].isin([dd])]
    df8 = df7[~df7['CUSTOMATTR15'].isin(['UNKNOWN'])]
    df9 = df8.reset_index()
    df10 = flk.countif(df9,'CUSTOMATTR15','CUSTOMATTR15','CNT')
    df10 = df10.drop_duplicates(subset='CUSTOMATTR15', keep='first')
    df01 = fst.add_col_df(df10,'GT02')
    df11 = fst.add_col_df(df10,'GTLT')
    df11['GTLT'] = df11.apply(lambda row: 'GT10' if (row.CNT>=10) else "LT10", axis = 1)
    df11 = df11[df11['GTLT'].isin(['GT10'])]
    df12 = df11[['CUSTOMATTR15','CNT']]
    df13 = get_region(df12)
    getsms = smsprep_znwise(df13,'Region','CUSTOMATTR15','CNT')
    chtid = "-1001213797107"
    msghead1 = "RRU Theft Alarm Occurance 10 Plus"
    msghead2 = "on " + odt.day_minus(1)
    msghead = msghead1 + chr(10) + msghead2 + chr(10) + chr(10) + "SiteCode: Counts " + chr(10) + chr(10) + getsms
    fmsg = msghead.replace(chr(10),"%0a")
    custom_msg_sender("-1001213797107",fmsg)
    return 'done'
