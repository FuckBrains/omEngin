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
                

print(find_lastest_date(mydf)) #change mydf to yours






'102','4/12/2020 4:52','3/12/2020 16:46','1/12/2020 16:46','4/12/2020 1:08','3/12/2020 12:40'
'501','3/12/2020 16:43','3/12/2020 16:44','3/12/2020 16:39','3/12/2020 16:43','4/12/2020 1:24','4/12/2020 4:46'
'603',3/12/2020 12:27				4/12/2020 1:51	4/12/2020 5:11
501	3/12/2020 12:29	3/12/2020 16:34	3/12/2020 23:53	4/12/2020 0:07	4/12/2020 2:18	4/12/2020 5:42




