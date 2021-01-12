import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import os, sys
import argparse

parser = argparse.ArgumentParser(description='Script so useful.')

def read_csv_xls(pth, sht = None):
    df = ''
    if sht == None:
        df = pd.read_csv(pt)
    else:
        df = pd.read_excel(pt, sheet_name = sht)
    print(df)
    
args = parser.parse_args()
print(args)