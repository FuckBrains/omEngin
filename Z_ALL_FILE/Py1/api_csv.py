import requests
import pandas as pd
import os
import csv
import io

pth = os.getcwd() + '\\DW\\'
cf = pth + 'hideme.csv'
hideme_access = "730480402242392"
hideme = "http://incloak.com/api/proxylist.php?country=US&Speed<=1000&ports=&type=socks5&out=csv&code=" + hideme_access
lnFF = "http://filefab.com/api.php?l=90Ft8r4B9ejHAmXjfUKDcoNTZIZrCPGyqv-0E2JAx_Q"

def csv_read(cf):
    with open(cf, newline='') as csvfile:
        reader = csv.DictReader(csvfile,delimiter=';')
        for row in reader:
            print(row['ip'],row['port'],row['city'])

def csv_2df(path,delim):
    df = pd.read_csv(path,delimiter=delim)
    return df

def csv_2dict(path,lst_fieldname):
    with open(path, newline='') as csvfile:
        fieldnames = lst_fieldname
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        return writer

def api_csv_read(lnk,delim):
    download = requests.get(lnk)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=delim)
    lst = list(cr)
    for row in lst:
        print(row)

def api_csv_df(lnk,delim):
    urlData = requests.get(lnk).content
    df = pd.read_csv(io.StringIO(urlData.decode('utf-8')),delimiter=delim)
    return df

#x = api2df(hideme,";")
x = csv_2df(cf,";")
print(x)