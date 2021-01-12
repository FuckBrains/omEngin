import requests as r
import json
from pprint import pprint
import os

def ipdb_1(ip):
    #https://app.ipgeolocation.io/
    ipgeolocation = "ec875907f0da48b3ac1859a1096c5971"
    apilink = 'https://api.ipgeolocation.io/ipgeo?apiKey=' + ipgeolocation + "&ip=" + ip
    G = r.get(apilink)
    print(G.text)
    return G.text


def ipdb_2(ip):
    url = "https://freegeoip.app/json/" + ip
    headers = {
        'accept': "application/json",
        'content-type': "application/json"
        }
    response = r.request("GET", url, headers=headers)
    print(type(response))
    print(response.text)

def ipdb_filefab():
    z = "http://filefab.com/api.php?l=90Ft8r4B9ejHAmXjfUKDcoNTZIZrCPGyqv-0E2JAx_Q"
    G = r.get(z)
    print(G)

def ip_geojs(ip):
    os.system("curl https://get.geojs.io/v1/ip/geo/{" + ip + "}.js")

def ip_db3(ip):
    rs = r.get('https://bgp.tools/ip?q=' + ip).text
    print(rs)


#x = ipdb_1('45.156.24.78')
#ipdb_2('45.156.24.78')
ipdb_2('45.156.24.78')
