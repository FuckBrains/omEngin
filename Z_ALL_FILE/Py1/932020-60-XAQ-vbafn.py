#import MySQLdb
import pandas as pd
import os
import numpy

#conn= MySQLdb.connect("localhost","root","admin","omdb")
#df = pd.read_sql("select * from sitedb",conn)
file = os.getcwd() + "\\" + "RobiLive.csv"

class pyvb:
    def __init__(self, dic):
        self.df = pd.DataFrame(dic)
        self.arr = self.df.to_numpy()
        self.lst = list(self.df.columns.values)
    def PrintDf(self):
        print(self.df)
    def print_all_row_comm_seperated(self):
        lrw = (self.arr).shape[0]
        lcol = (self.arr).shape[1]
        i = 0
        hp = ''
        heap = ''
        while i < lrw:
            hp = ''
            j = 0
            while j < lcol:
                if hp == '':
                    hp = str(self.arr[i][j])
                else:
                    hp = hp + ', ' + str(self.arr[i][j])
                j = j + 1
            heap = heap + '\n' + str(hp)
            i = i + 1
        return heap
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
        df2 = self.df[self.df[colname].str.contains(strval, na=False)]
        return df2.to_dict()
    def make_qry_str_sitecode(self,colname):
        lst = self.df[colname].to_list()
        hp = 0
        n = 0
        for i in lst:
            n = n + 1
            if n == 1:
                hp = "'" + i + "'"
            else:
                hp = hp + ',' + "'" + i + "'"
        return hp
    def vbprint_row_after_row(self, colinlist):
        hd = ''
        for x in colinlist:
            if hd == '':
                hd = x
            else:
                hd = hd + ', ' + x
        ndf = self.df[colinlist]
        cnt = 0
        heap = ''
        for r in range(ndf.shape[0]):
            count = 0
            for c in range(ndf.shape[1]):
                if count == 0:
                    hp = str(ndf.iloc[r, c])
                else:
                    hp = hp + ', ' + str(ndf.iloc[r, c])
                count = count + 1
            if cnt == 0:
                heap = hd + '\n' + hp
            else:
                heap = heap + '\n' + hp
            cnt = 1
        return heap
    def vbprint_col_comma(self, colinlist):
        ndf = self.df[colinlist]
        cnt = 0
        heap = ''
        for r in range(ndf.shape[0]):
            count = 0
            for c in range(ndf.shape[1]):
                if count == 0:
                    hp = str(ndf.iloc[r, c])
                else:
                    hp = hp + ', ' + str(ndf.iloc[r, c])
                    count = count + 1
            if cnt == 0:
                heap = hp
            else:
                heap = heap + '\n' + hp
            cnt = 1
            hp = ''
        print(heap)
    def vbrht(self):
        print('x')
    def vblft(self):
        print('x')
    def vbinstr(self):
        print('x')
    def vbmid(self):
        print('x')
    def vbdatediff(self):
        print('x')
    def vbreplace(self):
        print('x')




#dfc = pd.read_csv(file)
#dic = dfc.to_dict()

#mli = ['LastOccurrence', 'Tally','CustomAttr11']
#pv.Row_Item_From_List(9,mli)

#pv2 = pyvb(dic,mli)
#pv.PrintDf()
#pv2.PrintDf_ByList()
#gval = pv.MatchParse('DHKTL04','CustomAttr15','Resource','Summary','LastOccurrence')
#print(gval)
#print(pv.VbMatch_Col('DHKTL04',3))
#print(pv.VbMatch_Row('CustomAttr15',0))
#pv.PrintLst()
#df = pd.read_csv(file)
#print(df)
#dic = df.to_dict()
#lst = ['Site Code','LTE Status','Priority']
#pv2 = pyvb(dic)
#print(pv2.print_all_row_comm_seperated())
#print(pv2.vbprint_row_after_row(lst))
#print(pv.make_qry_str('INTERNALLAST'))
#pv.make_qry_str(lst)
