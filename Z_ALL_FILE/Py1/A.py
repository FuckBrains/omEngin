import pandas as pd
import numpy as np
from datetime import *
from dateutil.parser import *

def find_lastest_date(dataframe):
    lss = []
    max_date = []
    df = dataframe.astype(str)
    for row in range(len(df)):
        for col in df:
            try:
                lss.append(parse(str(df.loc[row,col])))
            except:
                pass
        try:
            max_date.append(max(lss).strftime("%Y/%m/%d %H:%M"))  #change format for output column
        except:
            max_date.append("could not parse date from string")
    else:
        return dataframe.assign(lastest_date = np.array(max_date))
                

print(find_lastest_date(df1)) #change df1 to your