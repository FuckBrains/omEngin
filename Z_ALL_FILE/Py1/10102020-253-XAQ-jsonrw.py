#ref:https://pynative.com/python-json-dumps-and-dump-for-json-encoding/
import json
import os

def jsonMod(jsn,ip,port):
    with open(jsn, "r") as jsonFile:
        x = json.load(jsonFile)
        x['configs'][0]['server'] = ip
        x['configs'][0]['server_port'] = port
        print(x)
    with open(jsn, "w") as jsonFile:
        json.dump(x, jsonFile)

jp = os.getcwd() + "\\proxylist.json"
jsonMod(jp,'8.8.8.8','01010')

#demjson.decode(prot)
#(prot)
#print(usr)

