import csv
import pandas as pd
import io
import requests

def api_hideme():
    hideme = "http://incloak.com/api/proxylist.php?out=csv&code=" + "730480402242392"
    urlData = requests.get(hideme).content
    df = pd.read_csv(io.StringIO(urlData.decode('utf-8')),delimiter=";")
    return df

def api_premproxy():
    z = "http://filefab.com/api.php?l=90Ft8r4B9ejHAmXjfUKDcoNTZIZrCPGyqv-0E2JAx_Q"
    urlData = requests.get(z).content
    df = pd.read_csv(io.StringIO(urlData.decode('utf-8')), delimiter=":")
    return df

def dailyproxy():
    url = "https://proxy-daily.com/api/getproxylist?apikey=MHAvkX-UOWjz6vbT-t9cpK1&format=ipport&country=US&type=socks5&lastchecked=60"
    urlData = requests.get(url).content
    df = pd.read_csv(io.StringIO(urlData.decode('utf-8')), delimiter=":")
    return df

def openproxy():
    url = "https://api.openproxy.space/premium/plain?amount=34999&apiKey=i9414-d994p4Pa29118LW-yfIl5-eBY64dMT5N16uDv-Vw10n&checksMore=354&countries=US&protocols=3&status=1&streak=1"
    urlData = requests.get(url).content
    df = pd.read_csv(io.StringIO(urlData.decode('utf-8')), delimiter=":")
    return df
