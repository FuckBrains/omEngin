import pandas as pd
import numpy as np







def add_list_as_column(df,nlst):
    #ls = df.values.tolist()
    df = df.append(pd.DataFrame(nlst,columns=['col1','col2']),ignore_index=True)
    print(df)




l0 = ["0", "1", "2", "3", "4"]
l1 = ["Amar", "Barsha", "Barsha", "Tanmay", "Misbah"]
l2 = ["Alpha", "Bravo", "Charlie", "Tango", "Mike"]
l4 = [["Amar", "Barsha", "Carlos", "Tanmay", "Misbah"],["Alpha", "Bravo", "Charlie", "Tango", "Mike"]]
l5 = ['A','B','C','D','E']
l6 = ['DHK', 'RAJ', 'CTG', 'SYL', 'MYN']
l7 = [['DHK, P1'], ['DHK, P2'] , ['DHK, P3'] , ['DHK, P4'] , ['DHK, P5']]

df1 = join_list(l0, l1, l2)
df1.columns = ['1','2','3']
dc1 = conv_lst_dic(l0,l6)
#print(dc1)
dc2 = conv_lst_dic(l0,l7)
#print(dc2)
l8 = df1['1']
l9 = df1 [['2','3']]
l10 = l9.values.tolist()
dc3 = conv_lst_dic(l0,l7)
#print(dc3)
#print(df1)
df2 = pd.DataFrame(dc3)
#print(df2)

x = vlookup(df1,dc3,'1','TOTO')

#print(x)
ts = x['2']
lx = ts.values.tolist()
cnt = lx.count('Barsha')
#print(cnt)
x = countif(x,'2','2',"ONCOL2")
#p = add_col_df(df,'Test',1)
#add_list_as_column(df,l4)
#datatype_conversion = df['Customer Number'].astype('int')


def conct(a1,a2):
    ls = []
    strn = ""
    for i in range(len(a1)):
        strn = strn + str(a1[i]) + str(a2[i])
    return strn

def conct1(arg1,arg2):
    if isinstance(arg1, list) and isinstance(arg2, list):
        ls = []
        for i in range(arg1):
            ls.append(str([i]) + str(arg2[i]))
        return ls
    else:
        ag1 = arg1.values.tolist()
        ag2 = arg2.values.tolist()
        ls = []
        for i in range(ag1):
            ls.append(str([i]) + str(ag2[i]))
        return ls

def countifz(df,*argv):
    if isinstance(df,pd.DataFrame):
        if len(argv) % 2 != 0:
            print('need conditions for every ref range have')
        else:
            rng = len(argv) / 2
            i = 0
            j = 2
            A1 = ""
            B1 = ""
            X1 = []
            X2 = []
            while i < rng:
                if j > rng:
                    A1 = A1 + (df[argv[i]])
                    B1 = B1 + (df[argv[i+1]])
                    i = i + 2
                else:
                    A1 = A1 + (conct(df[argv[i]],df[argv[j]]))
                    B1 = B1 + (conct(df[argv[i+1]],df[argv[j+1]]))
                    i = i + 2
                    j = j + 2
                X1.append(A1)
                X2.append(B1)
            print(X1,X2)
    else:
        print('first parameter must be dataframe (full data range)')


countifz(x, '1', '1', '3', '3')
