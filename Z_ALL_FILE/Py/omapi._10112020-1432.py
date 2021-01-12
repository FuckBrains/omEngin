import csv
import pandas as pd
import io
import requests
import nmap
import pydnsbl
import urllib
import socket
import json
import requests
socket.setdefaulttimeout(180)
import os

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

def auth0(ip):
    try:
        url = "https://signals.api.auth0.com/badip/" + ip
        headers = {
            'accept': "application/json",
            'x-auth-token': "51fac7a1-04c8-4c2f-8143-76c5fa498ff9"
            }
        response = r.request("GET", url, headers=headers)
        x = json.loads(response.text)
        return x['type']
    except:
        print('err')
    finally:
        return "NA"


def isblk(ip):
    ip_checker = pydnsbl.DNSBLIpChecker()
    x = str(ip_checker.check(ip))
    print(x)
    if 'BLACKLISTED' in x:
        a = x.rfind('(')
        b = x.rfind(')')
        ab = x[a+1:b]
        ap = auth0(ip)
        return 'black - ' + ab + ' - ' + str(ap)
    else:
        return 'fine'

def islive(ip,port):
    qry = 'nmap -p ' + str(port) + ' ' + str(ip)
    y = os.popen(qry).read()
    print(y)
    if 'open' in y:
        ab = isblk(ip)
        x = 'live' + '-' + str(ab)
    else:
        x = 'dead'
    return x

def ipdb_2(ip):
    url = "https://freegeoip.app/json/" + ip
    headers = {
        'accept': "application/json",
        'content-type': "application/json"
        }
    response = requests.request("GET", url, headers=headers)
    x = json.loads(response.text)
    y = x['city'] + ' -' + x['country_code']
    return y


islive('173.0.54.188','6888')
