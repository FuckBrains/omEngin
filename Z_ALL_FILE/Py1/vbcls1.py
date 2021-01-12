import pandas as pd
import numpy as np


class omdf:
    def __init__(self,dff):
        self.df = dff
        self.arr = self.df.to_numpy()
    def df_add_col_instr(self):
        self.df['cat'] = self.df.apply(lambda row: TS(row.Summary), axis = 1)
        return self.df.to_dict()
    def df_add_col_dic(self,colname,newcol,dic):
        self.df[newcol] = self.df['scode'].map(dic)
        return self.df.to_dict()
    def df_add_col_slice_str(self,newcolname):
        self.df[newcolname] = self.df.apply(lambda x : x.CustomAttr15[0:5], axis = 1)
        return self.df.to_dict()
    def df_rmv_column(self,lis):
        ndf = self.df[lis]
        return ndf.to_dict()
    def df_countif(self,column_name,newcolumn_name):
        code = pd.Series(self.df[column_name])
        lst = code.values.tolist()
        dic = {}
        for i in lst:
            dic[i] = lst.count(i)
        df_occ = pd.DataFrame(dic.items(),columns=[column_name, newcolumn_name])
        mdf = self.df.merge(df_occ, on=column_name)
        return mdf
    def df_instr(self,colname,srcstr):
        self.df[srcstr] = list(map(lambda x: x.count(srcstr), self.df[colname]))
        return self.df
    def df_vlookup(self,df2,common_colname):
        mdf = self.df.merge(df2, on=common_colname)
        return mdf




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
        df2 = self.df[self.df[colname].str.contains(strval, na=False)]
        return df2.to_dict()