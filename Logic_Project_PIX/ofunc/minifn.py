import time as tm
import pandas as pd
import numpy as np
from datetime import *
import os

# getkey(my_dict, ky)



def getkey(my_dict, ky):
    if ky is not None:
        for key, value in my_dict.items ():
            if key in str (ky):
                return value
        else:
            return "key not found"

def append_dic_value(dict_obj, key, value):
    if key in dict_obj:
        if not isinstance(dict_obj[key], list):
            dict_obj[key] = [dict_obj[key]]
        dict_obj[key].append(value)
    else:
        dict_obj[key] = value

def dic_by_key(dc, ky):
    hp = ky + " : "
    for key in dc:
        if key == ky:
            ls = dc[key]
            if isinstance(ls, list):
                for i in range(len(ls)):
                    if ls[i] not in hp:
                        hp = hp + chr(10) + ls[i]
                    else:
                        pass
                else:
                    if len(hp) < 8:
                        return "3G - 0"
                    else:
                        return chr(10) + hp
                    exit()
            elif ls is None or ls == '':
                return hp + " 0" + chr(10)
                exit()
            else:
                try:
                    hp = chr(10) + hp + chr(10) + ls
                    return hp
                    exit()
                except:
                    return hp + " 0" + chr(10)
                    exit()

