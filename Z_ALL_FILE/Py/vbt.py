import pandas as pd
import numpy as np
import os
import func.fnfn as fn

pt1 = os.getcwd() + "\\refdb\\S30.csv"
pt2 = os.getcwd() + "\\refdb\\S1800_200.csv"

df = pd.read_csv(pt2)
#print(df.columns)
#df = df.assign(new_column = "NA") # column inserted at last, here new_column = column name and "NA" = rows value of new column

def concat(df, column_1, column_2, new_column_name):
    df = df.assign(new_column = "NA")
    df.rename(columns = {'new_column': new_column_name}, inplace = True)
    for i in range(len(df)):
        data_1 = df.loc[i,column_1]
        data_2 = df.loc[i,column_2]
        df.loc[i,new_column_name] = str(data_1) + str(data_2)
    return df

def countif(col_as_range,criteria):
    # col_as_range can be list or daraframe series
    if isinstance(col_as_range,list):
        count = col_as_range.count(criteria)
        return count
    elif isinstance(col_as_range, pd.core.series.Series):
        col_range_list = col_as_range.values.tolist()
        count = col_range_list.count(criteria)
        return count
    else:
        return "none"


print(countif(df['CUSTOMATTR15'],"FNCGL06"))



def countif_apply_on_col(df0,ref_col_as_range,ref_col_for_Cells):
    if isinstance(ref_col_as_range,str):
        df = df0.assign(coln = 'NA')
        rdf = df[ref_col_as_range]
        reflst = rdf.values.tolist()
        vdf = df[ref_col_for_Cells]
        nwlst = []
        for i in vdf:
            try:
                count = reflst.count(i)
                nwlst.append(count)
            except:
                nwlst.append('0')
    df['coln'] = nwlst
    return df

def datediff(unit,datetime1,datetime2):
    d1 = ""
    d2 = ""
    try:
        if isinstance(datetime1, str):
            d1 = parse(datetime1)
        elif isinstance(datetime1, datetime):
            d1 = datetime1
        if isinstance(datetime2, str):
            d2 = parse(datetime2)
        elif isinstance(datetime2, datetime):
            d2 = datetime2
        if unit == 'n':
            return round(abs((d1 - d2)).total_seconds()/60,3)
        elif unit == 'h':
            return round(abs((d1 - d2)).total_seconds()/3600,3)
        elif unit == 's':
            return round(abs((d1 - d2)).total_seconds(),3)
        elif unit == '':
            return abs(d1 - d2)
        elif len(unit)>3:
            x = abs(d1 - d2)
            print(x)
            try:
                return datetime.strftime(x,"%Y%M%d")
            except:
                return "format not appropriate"
    except:
        return "NA"


xx = countif_apply_on_col(df,'CUSTOMATTR15','CUSTOMATTR15')
print(xx)
