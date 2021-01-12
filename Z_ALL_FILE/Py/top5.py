import pandas as pd
import numpy as np
import os
import requests
import func.lookup as look
import func.fnstr as fst
import func.fndatetime as fdt
import func.fnlook as flk
import func.omdtfn as odt
import func.fnfn as fn
import db.db as sq
import prep as pr
from datetime import *


def custom_msg_sender_top5(chatid,msg):
    TOKEN = "961035316:AAGWIlt5GjIkBz1QI1s6WKbwVnfubmn0m6E"
    url = "https://api.telegram.org/bot" + TOKEN + "/sendMessage?chat_id=" + str(chatid) + "&text=" + msg
    requests.get(url)


TSS = lambda x : '2G' if ('2G SITE DOWN' in x) \
                else ('3G' if ('3G SITE DOWN' in x) \
                else ('4G' if ('4G SITE DOWN' in x) \
                else "other"
                ))

def map_customer(df):
    df4 = df
    df4 = df4.rename(columns={'CUSTOMATTR15':'CODE'})
    pt = os.getcwd() + "\\db\\T70.csv"
    df5 = pd.read_csv(pt)
    df6 = flk.vlookup(df4,df5,'CODE','NA')
    return df6

def mod_techwise(df14):
    try:
        df15 = df14.groupby(df14['CODE']).MTTR.sum().to_frame(name = 'SMX').reset_index()
        df17 = df14.merge(df15, on='CODE')
        df17 = df17.drop_duplicates(subset='CODE',keep='last', inplace = False)
        df17['SMX'] = df17['SMX'].round(decimals=2)
        df18 = df17.sort_values('CODE')
        df19 = df18[['CODE','CName','CNT','SMX']]
        arr = df19.to_numpy()
        rw, col = arr.shape
        stx = ""
        for i in range(rw):
            if arr[i][3] > 2:
                if stx == "":
                    stx = arr[i][1] + chr(10) + arr[i][0] + ': ' + str(arr[i][2]) + '/' + str(arr[i][3]) + ' min'
                else:
                    stx = stx + chr(10) + chr(10) + arr[i][1] + chr(10) + arr[i][0] + ': ' + str(arr[i][2]) + '/' + str(arr[i][3]) + 'min'
        return stx
    except:
        return "NA"

def top5_outage_report(df0):
    df1 = df0
    df = fst.add_col_df(df1,'cat')
    df['cat'] = df.apply(lambda row: TSS(row.SUMMARY) , axis = 1)
    df2 = df[~df['cat'].isin(['other'])]
    df4 = fst.add_col_df(df2,'DT')
    df4['DT'] = df4.apply(lambda x : pd.to_datetime(x.LASTOCCURRENCE).strftime("%d-%b-%Y"), axis = 1)
    df6 = fst.add_col_df(df4,'CLRYR')
    df6['CLRYR'] = df6.apply(lambda x : pd.to_datetime(x.CLEARTIMESTAMP).strftime("%Y"), axis = 1)
    dd = odt.day_minus_dy(1)
    df7 = df6[df6.DT.str.contains(dd) & df6.CLRYR.str.contains('2020')]
    df11 = map_customer(df7)
    df12 = flk.countif(df11,'CName','CName','CNT')
    df13 = df12[['CODE','LASTOCCURRENCE','CLEARTIMESTAMP','cat','CName','GName', 'CNT']]
    df14 = fdt.datedif(df13,'MTTR','LASTOCCURRENCE','CLEARTIMESTAMP')
    df2G = df14[df14.cat.str.contains('2G')]
    df3G = df14[df14.cat.str.contains('3G')]
    df4G = df14[df14.cat.str.contains('4G')]
    G2 = "2G: " + chr(10) + mod_techwise(df2G)
    G3 = "3G: " + chr(10) + mod_techwise(df3G)
    G4 = "4G: " + chr(10) + mod_techwise(df4G)
    GG = G2 + chr(10) + chr(10) + G3 + chr(10) + chr(10) + G4
    fstx = "VIP TOP 5 Sites" + chr(10) +"Outage Count and Durtaion " + chr(10) + "on " + odt.day_minus(1) + chr(10) + chr(10) + "code: count/sum of duration" + chr(10) + chr(10) + GG
    fmsg1 = fstx.replace("&","and")
    fmsg = fmsg1.replace(chr(10),"%0a")
    custom_msg_sender_top5("-352454352",fmsg)
    print('top5 done')
