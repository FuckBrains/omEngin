import pandas as pd
import numpy as np
import os
import requests
import func.lookup as look
import func.fnstr as fst
import func.fndatetime as fdt
import func.fnlook as flk
import func.fnfn as fn
import db.db as sq
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


def theft_occ(df1):
    df = fst.add_col_df(df1,'cat')
    df['cat'] = df.apply(lambda row: 'TH' if ('THEFT' in row.SUMMARY) else "other", axis = 1)
    df2 = df[~df['cat'].isin(['other'])]
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
