import json
import requests
import os
from pprint import pprint

def api_ip2asn(ip):
    url = "https://api.iptoasn.com/v1/as/ip/" + ip
    x = requests.get(url)
    y = x.json()
    return y["as_description"]    
api_ip2asn("38.114.23.4")