import pandas as pd
import numpy as np
import os
from datetime import *
pd.options.mode.chained_assignment = None  # default='warn'

pt = os.getcwd()
alarm = "E:\\GIT\\OmProject\\OmPY\\omfn4\\C.csv"

df0 = pd.read_csv(alarm)
df1 = df0[['SERIAL','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','CUSTOMATTR3']]




df1 = df0[['SERIAL','CUSTOMATTR15','SUMMARY','LASTOCCURRENCE','CLEARTIMESTAMP','CUSTOMATTR3','IDENTIFIER']]
#xxx = str_cut(df1,df1['CUSTOMATTR15'],'shortcode',0,5)
lx = ['2G SITE','3G SITE']
dc = {'2G SITE':'2G','3G SITE':'3G'}
dc2 = {'HUW-2G SITE DOWN':"HW",'ERI-3G SITE DOWN':'ERI'}
#aq = filter_p(df1,lx,'SUMMARY')
#print(aq['SUMMARY'])
#aw = filter_p_ncol(df1,dc,'SUMMARY','cat')
#print(aw)
aqq = vlookup(df1,dc2,'SUMMARY','VLOOKUP')
print(aqq)
#print(aqq.loc[(aqq['VLOOKUP']=='ERI')])
#print(aqq.columns)
#x = df_add_col(df1,'scode','codecut')
#print(x)
#y = filter_e_2col(aqq,'SUMMARY','ERI-2G SITE DOWN','VLOOKUP','ERI',)
#x = countifs(aqq,'SUMMARY','ERI-3G SITE DOWN','VLOOKUP','ERI')
#print(y)
lst = ['SUMMARY','VLOOKUP']
za = aqq.drop_duplicates(subset=lst)
#print(za)

asq = datedif(df1,'AG','LASTOCCURRENCE')
#print(asq)

sm = sumifs(asq,'CUSTOMATTR15','AG')
print(sm)
