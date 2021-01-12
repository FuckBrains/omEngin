import pandas as pd
import numpy as np
import os


pt1 = os.getcwd() + "\\refdb\\S30.csv"
pt2 = os.getcwd() + "\\refdb\\S1800_200.csv"

df1= pd.read_csv(pt2)
print(df1)
