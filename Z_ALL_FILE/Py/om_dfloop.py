import pandas as pd
import numpy as np
import os

##https://www.delftstack.com/howto/python-pandas/how-to-iterate-through-rows-of-a-dataframe-in-pandas/

pt = os.getcwd()
FL1 = "E:\\GIT\\OmProject\\OmSocks\\T3.csv"
dff = pd.read_csv(FL1)

#Procedure 1: index
def dflp_rw_using_index(df):
    for i in df.index:
        print(df['ip'][i], df['port'][i])

#Procedure 2: loc
def dflp_rw_using_loc(df):
    for i in range(len(df)):
        print(df.loc[i,"ip"])

#Procedure 3: iloc
def dflp_rw_using_iloc(df):
    for i in range(len(df)):
        print(df.iloc[i,0])

#Procedure 4: iterrows
def dflp_rw_using_iterrows(df):
    for indx, row in df.iterrows():
        print(row['ip'],row['port'])

#addcol type 1
def df_add_col_1(df):
    for i in range(len(df)):
        df.loc[i,"NWcol"] = df.loc[i,"port"] + 4
    return df

def addtest(x,y):
    z = x + ":" + str(y)
    return z

#addcol type 2
def df_add_col_2(df):
    for i in range(len(df)):
        df.loc[i,"ip_port"] = addtest(df.loc[i,"ip"], df.loc[i,"port"])
    return df

#addcol type 3
def df_add_col_3(df,colname):
    df = df.assign(colname = lambda row: row['port'] + 4 , axis=1)
    return df

def df_add_row_1(df,content_lst):
    df.loc[df.shape[0]+1] = content_lst
    return df

def df_del_row_cond_1(df,colname,ontxt):
    df.replace('', np.nan)
    indx = df[ df[colname] != ontxt ].index
    df.drop(indx , inplace=True)
    return df

#dff['NWcol'] = np.nan
#derive_col_with_cond(dff)
#df_add_row_1
#lst = ['0.0.0.0','1111','Kiskunlachaza -HU','1111','0.0','188.6','29582','us']
#print(df_add_row_1(dff,lst))
#print(df)
dx = df_del_row_cond_1(dff,"ip",'41.76.157.202')
print(dx.columns.to_list())
print(dx.columns.values.tolist())

#xd.to_csv("E:\\GIT\\OmProject\\OmSocks\\T4.csv")
#print(dff)
# dff.columns = ['ip','port'] #(set columns names)
# dff.iloc[0,0] = "New Val" #(change values in df cells using iloc)
# dff.loc[1,"ip"] = "XXXXX" #(change values in df cells using loc)
# print(dff)

#dff['NWcol'] = np.nan #(add empty column)
#print(dflp_add_col_2(dff))
def df_col_2_list_1(df):
    xx = df.apply(lambda row: row['port'] + 4 , axis=1)
    print(xx.to_list())

def df_2_list(df):
    df0 = df['ip']  #serialization
    df1 = df['port']  #serialization
    lst1 = df0.to_list()  #Convert into list
    lst2 = df1.to_list()  #Convert into list
    lst1.append(lst2)

df_2_list(dff)
