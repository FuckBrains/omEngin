import pandas as pd
import numpy as np
import os
import sys

def conv_datatype():
    print('x')

def dfmt1(arg):
    x = isinstance(arg, (str, float, int, str, list, dict, tuple, pd.DataFrame, np.ndarray))
    if type(x) == pd.DataFrame:
        df0 = arg
    elif type(x) ==np.ndarray:
        nar = arg
    elif type(x) == dict:
        df0 = pd.Dataframe(arg)
        print(df0)

def dfmt2(df, whatto):
    if whatto == 'col':
        print('df frame columns name')
        print('1 . df columns \n', df.columns)
        print('\n 2. columns into list \n' ,list(df.columns))
        print('\n No of Colummns', df.shape[1])
        print('\n slice columns ', df0)
    if whatto == 'row':
        print(df.shape[0])








FL1 = "E:\\GIT\\OmProject\\OmPY\\omfn\\sem_raw.csv"
dff = pd.read_csv(FL1)
dic = dff.to_dict()
nar = dff.to_numpy()
dk = dff.columns[['Severity','PG_Restricted']]
#df0 = dff.columns[['Serial','Summary']]
#dfmt2(dff,'col')
