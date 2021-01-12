#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import numpy
import MySQLdb
import omdtfn as odt

#conn= MySQLdb.connect("localhost","root","admin","omdb")
#df_mysql = pd.read_sql("select * from sitedb",conn)
omdb = os.getcwd() + "\\" + "OMDB.csv"

pntxt = os.getcwd() + "\\" + "Periodic_Notification.txt"
pth = os.getcwd() + "\\" + "WRT1.csv"
pth2 = os.getcwd() + "\\" + "WRT2.csv"

#lambda <args> : <return Value> if <condition > ( <return value > if <condition> else <return value>)
TS = lambda x : '2G' if ('2G SITE DOWN' in x)                 else ('3G' if ('3G SITE DOWN' in x)                 else ('4G' if ('4G SITE DOWN' in x)                 else ('MF' if ('MAIN' in x)                 else ('DC' if ('VOLTAGE' in x)                 else ('TM' if ('TEMPERATURE' in x)                 else ('SM' if ('SMOKE' in x)                 else ('GN' if ('GEN' in x)                 else ('GN' if ('GENSET' in x)                 else ('TH' if ('THEFT' in x)                 else ('2_CELL' if ('2G CELL DOWN' in x)                 else ('3_CELL' if ('3G CELL DOWN' in x)                 else ('4_CELL' if ('4G CELL DOWN' in x)                 else "NA"))))))))))))

def write2txt(flname,txt):
    fo = open(flname,"w+")
    txt = fo.write(txt)
    fo.close()

class omdf:
    def __init__(self,dic):
        self.df = pd.DataFrame(dic)
        self.arr = self.df.to_numpy()
        self.lst = list(self.df.columns.values)
        self.aList = []
    def df_addcol_lamda(self):
        self.df['cat'] = self.df.apply(lambda row: TS(row.Summary), axis = 1)
        return self.df.to_dict()
    def df_addcol_fdic(self,d,newcolname):
        self.df[newcolname] = self.df['scode'].map(d)
        return self.df.to_dict()
    def df_apply_on_col(self,newcolname):
        self.df[newcolname] = self.df.apply(lambda x : x.CustomAttr15[0:5], axis = 1)
        return self.df.to_dict()
    def df_remove_col_by_list(self,lis):
        ndf = self.df[lis]
        return ndf.to_dict()

def PNPW(dic,lis):
        ndf = pd.DataFrame(dic)
        ar = ndf.to_numpy()
        lcol = (ar).shape[1]
        j = 0
        G2T = 0
        G3T = 0
        G4T = 0
        heap = ""
        for i in lis:
            g2 = ndf[ndf['cat'].str.contains('MF') & ndf['Zone'].str.contains(lis[j])]
            g3 = ndf[ndf['cat'].str.contains('DL') & ndf['Zone'].str.contains(lis[j])]
            G2T = g2.shape[0] + G2T
            G3T = g3.shape[0] + G3T
            hd = str(lis[j]) + ": " + str(g2.shape[0]) + "/" + str(g3.shape[0])
            if j == 0:
                heap = hd
            else:
                heap = heap + '\n' + hd
            j = j + 1
        reg = 'Region: ' + 'MF/DL'
        Nat = 'National: ' + str(G2T) + '/' + str(G3T)
        heaps = reg + '\n' + Nat + '\n' + '\n' + heap
        return heaps

def ByCat(dic,lis,strval):
        ndf = pd.DataFrame(dic)
        ar = ndf.to_numpy()
        lcol = (ar).shape[1]
        j = 0
        G2T = 0
        heap = ""
        for i in lis:
            g2 = ndf[ndf['cat'].str.contains(strval) & ndf['Zone'].str.contains(lis[j])]
            G2T = g2.shape[0] + G2T
            hd = str(lis[j]) + ": " + str(g2.shape[0])
            if j == 0:
                heap = hd
            else:
                heap = heap + '\n' + hd
            j = j + 1
        heaps = "National: " + str(G2T) + '\n' + '\n' + heap
        return heaps
    
def PN_Format(dic,lis):
        ndf = pd.DataFrame(dic)
        ar = ndf.to_numpy()
        lcol = (ar).shape[1]
        j = 0
        G2T = 0
        G3T = 0
        G4T = 0
        heap = ""
        for i in lis:
            g2 = ndf[ndf['cat'].str.contains('2G') & ndf['Zone'].str.contains(lis[j])]
            g3 = ndf[ndf['cat'].str.contains('3G') & ndf['Zone'].str.contains(lis[j])]
            g4 = ndf[ndf['cat'].str.contains('4G') & ndf['Zone'].str.contains(lis[j])]
            G2T = g2.shape[0] + G2T
            G3T = g3.shape[0] + G3T
            G4T = g4.shape[0] + G4T
            hd = str(lis[j]) + ": " + str(g2.shape[0]) + "/" + str(g3.shape[0]) + "/" + str(g4.shape[0])
            if j == 0:
                heap = hd
            else:
                heap = heap + '\n' + hd
            j = j + 1
        hd = "Update of Site Down at " + odt.hrmin() + ' On ' + odt.dtmnyr()
        reg = 'Region: ' + '2G/3G/4G'
        Nat = 'National: ' + str(G2T) + '/' + str(G3T) + '/' + str(G4T)
        heaps = hd + '\n' + '\n' + reg + '\n' + Nat + '\n' + '\n' + heap
        return heaps

def PN(dicc):
    ls1 = ['CustomAttr15','Resource','Summary','LastOccurrence','BCCH']
    ls2 = ['Code','Zone']
    dfsingle = pd.DataFrame(dicc)
    dfomdb = pd.read_csv(omdb)
    dfs = dfsingle[ls1]
    dfdb = dfomdb[ls2]
    x1 = omdf(dfs)
    dfs1 = x1.df_addcol_lamda()
    dfx = pd.DataFrame(dfs1)
    dfx.to_csv(pth)
    x2 = omdf(dfs1)
    dfs2 = pd.DataFrame(x2.df_apply_on_col('Code'))
    mergedDf = dfs2.merge(dfdb, on='Code')
    #dff = mergedDf[mergedDf['BCCH'].str.contains('YES')]
    mergedDf.to_csv(pth2)
    ls3 = ['DHK_S','DHK_N','DHK_M','CTG_S','CTG_N','CTG_M','COM','NOA','SYL','MYM','BAR','KHL','KUS','RAJ','RANG']
    #print(ByCat(mergedDf.to_dict(),ls3,"4G"))
    txt = PN_Format(mergedDf.to_dict(),ls3)
    txtpw = PNPW(mergedDf.to_dict(),ls3)
    print(txtpw)
    #write2txt(pntxt,txt)
    return txt

single = os.getcwd() + "\\" + "DWRRU.csv"
df = pd.read_csv(single)
#dc = df.to_dict()
#print(PN(dc))
print(df)



# In[ ]:





# In[ ]:




