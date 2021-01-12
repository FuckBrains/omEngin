import json
import requests
import os

def find_from_dict(key, dictionary):
    for k, v in dictionary.iteritems():
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in find(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                for result in find(key, d):
                    yield result


def checkList(ele, prefix):
    for i in range(len(ele)):
        if (isinstance(ele[i], list)):
            checkList(ele[i], prefix+"["+str(i)+"]")
        elif (isinstance(ele[i], str)):
            printField(ele[i], prefix+"["+str(i)+"]")
        else:
            checkDict(ele[i], prefix+"["+str(i)+"]")

def checkDict(jsonObject, prefix):
    for ele in jsonObject:
        if (isinstance(jsonObject[ele], dict)):
            checkDict(jsonObject[ele], prefix+"."+ele)

        elif (isinstance(jsonObject[ele], list)):
            checkList(jsonObject[ele], prefix+"."+ele)

        elif (isinstance(jsonObject[ele], str)):
            printField(jsonObject[ele],  prefix+"."+ele)

def printField(ele, prefix):
    print (prefix, ":" , ele)


tele_sender = "https://api.telegram.org/bot1176189570:AAEfPi9TIZIbnhWi4Ko6KQev2Iv7UbMw5js/getupdates"
response = requests.get(tele_sender)
data = response.json()
print(type(data))

def get_all_values(nested_dictionary):
    for key, value in nested_dictionary.items():
        if type(value) is dict:
            get_all_values(value)
        else:
            print(key, ":", value)

def lp_dic(ddf1):
    for key in ddf1:
        print(key,ddf1[key])

pth = os.getcwd()
print(pth)
s1 = pth + '\\sample2.json'
with open(s1, "r") as jsonFile:
        x = json.load(jsonFile)
        get_all_values(x)

def json_loop(data):
    for element in data: #If Json Field value is a Nested Json
        if (isinstance(data[element], dict)):
            print('~~dict~~')
            checkDict(data[element], element)
        #If Json Field value is a list
        elif (isinstance(data[element], list)):
            print('~~list~~')
            checkList(data[element], element)
        #If Json Field value is a string
        elif (isinstance(data[element], str)):
            print('~~str~~')
            printField(data[element], element)

