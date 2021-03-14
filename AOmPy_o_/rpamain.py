import pandas as pd
import sys,os
from pathlib import Path
from lib_o_x.rpaclss import *

zn = ['DHK_M','DHK_N','DHK_O','DHK_S','CTG_M','CTG_N','CTG_S','COM','NOA','BAR','KHL','KUS','MYM','RAJ','RANG','SYL']

TS = lambda x : '2G' if ('2G SITE DOWN' in x) \
                else ('3G' if ('3G SITE DOWN' in x) \
                else ('4G' if ('4G SITE DOWN' in x) \
                else ('2G' if ('2G CELL DOWN' in x) \
                else ('3G' if ('3G CELL DOWN' in x) \
                else ('4G' if ('4G CELL DOWN' in x) \
                else ('2G' if ('OML FAULT' in x) \
                else "NA"))))))


def format_one(df = False):
    if df == False:
        df = pd.read_csv(os.getcwd() + "\\csv_o_\\sclick.csv") 
        print(df.shape[0])
    xx = RPA(df)
    xxd = xx.dfget()
    xxd['CAT2'] = xxd.apply(lambda row: TS(row.SUMMARY) , axis = 1)
    D1 = xxd[xxd['CAT2'].isin(['NA'])]
    D2 = xxd[xxd['CAT2'].isin(['2G','3G','4G'])]
    D1.to_csv("D1.csv")
    D2.to_csv("D2.csv")
    D2 = D2.reset_index()
    D1 = D1.reset_index()
    yy = RPA(D1)
    
    
    #xx.csvmap(os.getcwd() + "\\csv_o_\\vipsite.csv",'CUSTOMATTR15')
    #xx.timecal('LASTOCCURRENCE')
    #xx.timefmt('DUR','%H:%M:%S')
    #xx.add_cond('CAT',['2G','3G','4G'])
    #xx.add_cond('ATRB',['e.co'])
    #xx.apply_cond(['Company'], condition='NA', operation='remove') 
    #xx.msgitems(['sZone'])
    #xx.rpagen()
    
format_one()