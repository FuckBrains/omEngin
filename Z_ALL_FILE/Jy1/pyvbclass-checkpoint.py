#!/usr/bin/env python
# coding: utf-8

# In[9]:


import MySQLdb
import pandas as pd
import os
import numpy

conn= MySQLdb.connect("localhost","root","admin","omdb")
df = pd.read_sql("select * from sitedb",conn)
file = os.getcwd() + "\\" + "sem_raw.csv"

class pyvb:
    def __init__(self, dic, li=[]):
        self.df = pd.DataFrame(dic)
        self.arr = self.df.to_numpy()
        self.lst = self.df[li]
    def PrintDf(self):
        print(self.df)
    def PrintDf_ByList(self):
        print(self.lst)
    def MatchParse(self,zn,zncol,parsecol_1,parsecol_2,parsecol_3):
        hp = ""
        ndf = self.df[self.df[zncol].str.contains(zn, na=False)]
        for ind in ndf.index:
            code = str(ndf[parsecol_1][ind])
            lo = str(ndf[parsecol_2][ind])
            resource = str(ndf[parsecol_3][ind])
            hp = hp + " \n"  + code + " || " + lo + " || " + resource
        z = zn + ': \n' + hp
        return z
    def VbMatch_Col(self,search_val,colnum):
        lrw = (self.arr).shape[0]
        i = 0
        while i < lrw:
            if search_val == self.arr[i][colnum]:
                break
            i = i + 1
        return i
    def VbMatch_Row(self,search_val,rwnum):
        lcol = (self.arr).shape[1]
        i = 0
        while i < lcol:
            if search_val == self.arr[rwnum][i]:
                break
            i = i + 1
        return i
    def Row_Item_From_List(self,rwnum,lis):
        ndf = self.df[lis]
        ar = ndf.to_numpy()
        lcol = (ar).shape[1]
        j = 0
        heap = ""
        while j < lcol:
            hd = str(lis[j]) + ":" + str(ar[rwnum][j])
            if j == 0:
                heap = hd
            else:
                heap = heap + '\n' + hd
            j = j + 1
        return heap
    def VbFilter(self,colname,strval):
        df2 = self.df[self.df[colname].str.contains(str_positive, na=False)]
        return df2.to_dict()

dfc = pd.read_csv(file)
dic = dfc.to_dict()
pv = pyvb(dic)
mli = ['LastOccurrence', 'Tally','CustomAttr11']
pv.Row_Item_From_List(9,mli)

#pv2 = pyvb(dic,mli)
#pv.PrintDf()
#pv2.PrintDf_ByList()
#gval = pv.MatchParse('DHKTL04','CustomAttr15','Resource','Summary','LastOccurrence')
#print(gval)
#print(pv.VbMatch_Col('DHKTL04',3))
#print(pv.VbMatch_Row('CustomAttr15',0))
#pv.PrintLst()      


# In[ ]:





# In[ ]:





# In[ ]:




