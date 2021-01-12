import pandas as pd
import os
import omsql.InsUpd as ii



pt = os.getcwd() + '\\OMDB.csv'
df = pd.read_csv(pt)
cond = ['Zone', 'ULKA']
dfx = df[cond]
print(dfx)
