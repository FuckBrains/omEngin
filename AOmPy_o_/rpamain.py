import pandas as pd
import sys,os
from pathlib import Path
from lib_o_.rpaclss import *
sys.path.append(str(Path(os.getcwd()).parent.parent.absolute()))

zn = ['DHK_M','DHK_N','DHK_O','DHK_S','CTG_M','CTG_N','CTG_S','COM','NOA','BAR','KHL','KUS','MYM','RAJ','RANG','SYL']

def format_one(df = False):
    if df == False:
        df = pd.read_csv(os.getcwd() + "\\csv_o_\\sclick.csv") 
        print(df.shape[0])
    xx = RPA(df)
    xx.csvmap(os.getcwd() + "\\csv_o_\\vipsite.csv",'CUSTOMATTR15')
    #xx.timecal('LASTOCCURRENCE')
    #xx.timefmt('DUR','%H:%M:%S')
    #xx.add_cond('sZone',zn)
    xx.add_cond('CAT',['2G','3G','4G'])
    xx.apply_cond(['Company'], condition='NA', operation='remove') 
    xx.msgitems(['CUSTOMATTR15','LASTOCCURRENCE','Company'])
    xx.rpagen()