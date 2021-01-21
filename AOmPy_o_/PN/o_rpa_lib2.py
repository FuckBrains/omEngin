import pandas as pd
import numpy as np
import os
from datetime import *


sx = """91,42,42,42,42,42,42,95,95,95,95,42,42,95,95,42,42,95,95,42,42,32,95,42,42,32,42,42,42,42,42,42,42,33
42,42,42,42,42,32,47,32,95,95,32,92,124,32,32,92,47,32,32,124,42,124,32,124,42,42,42,42,42,42,42,42,42,33
42,42,42,42,42,124,32,124,32,32,124,32,124,32,92,32,32,47,32,124,42,124,32,124,42,42,42,42,42,42,42,42,42,33
42,42,42,42,42,124,32,124,32,32,124,32,124,32,124,92,47,124,32,124,42,124,32,124,42,42,42,42,42,42,42,42,42,33
42,42,42,42,42,124,32,124,95,95,124,32,124,32,124,32,32,124,32,124,42,124,32,124,42,42,42,42,42,42,42,42,42,33
42,42,42,42,42,42,92,95,95,95,95,47,92,95,124,42,42,124,95,124,42,124,95,124,42,42,42,42,42,42,42,42,93,33"""

def omnm():
    print(chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),
          chr(45),chr(45),chr(45),chr(45),chr(45))
    time.sleep(1)
    xx = sx.replace("42","32")
    yy = xx.replace("33","xx")
    ls = yy.split("xx")
    hp = ""
    for i in range(len(ls)):
        xz = ls[i].split(",")
        hp = ""
        for n in range(len(xz)):
            if xz[n] == "":
                print(hp)
                hp = ""
            else:
                hp = hp + chr(int(xz[n]))
    print(chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),
          chr(45),chr(45),chr(45),chr(45),chr(45))
        
omnm()
time.sleep(5)


def o_print(my_dict):
    for key in my_dict.items():
        x = my_dict.get(key)

def getvalue(my_dict, ky):
    if ky is not None:
        for key, value in my_dict.items ():
            if key in str (ky):
                return value
        else:
            return 0

TS = lambda x : '2G' if ('2G SITE DOWN' in x) \
                else ('3G' if ('3G SITE DOWN' in x) \
                else ('4G' if ('4G SITE DOWN' in x) \
                else ('MF' if ('MAIN' in x) \
                else ('DL' if ('VOLTAGE' in x) \
                else ('TM' if ('TEMPERATURE' in x) \
                else ('SM' if ('SMOKE' in x) \
                else ('GN' if ('GEN' in x) \
                else ('GN' if ('GENSET' in x) \
                else ('TH' if ('THEFT' in x) \
                else ('C2G' if ('2G CELL DOWN' in x) \
                else ('C3G' if ('3G CELL DOWN' in x) \
                else ('C4G' if ('4G CELL DOWN' in x) \
                else ('DOOR' if ('DOOR' in x) \
                else "NA")))))))))))))


