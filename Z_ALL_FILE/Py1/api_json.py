import requests
import pandas as pd
import os
import json
import io
from pprint import pprint
import pandas as pd
from pandas.io.json import json_normalize


pth = os.getcwd()
print(pth)
s1 = pth + '\\sample1.json'
s2 = pth + '\\sample2.json'
tele_sender = "https://api.telegram.org/bot1176189570:AAEfPi9TIZIbnhWi4Ko6KQev2Iv7UbMw5js/getupdates"

def json_read(pth):
    with open(pth, "r") as jsonFile:
        x = json.load(jsonFile)
        pprint(x)

def api_json_read(url):
    response = requests.get(url)
    json_rs = response.json()
    print(json_rs.keys())
    


#json_read(s1)
api_json_read(tele_sender)