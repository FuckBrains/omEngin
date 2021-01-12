from datetime import *
import os
import pandas as pd
import numpy as np

def help_ofn1():
    ls =["write_into_text(contents, operation='write', filename = 'excmd', fpath = None) content can be list/string\n"]
    print(ls)

#df = df2.reset_index()

def append_into_textfile(text):
    nx = datetime.now ()
    file1 = os.getcwd() + "\\" + nx.strftime("%m%d%H%M%S") + ".txt"
    file2 = os.getcwd() + "\\dump\\" + nx.strftime("%m%d%H%M%S") + ".txt"
    try:
        try:
            f = open(file2, 'a+')
            print(file2)
        except:
            f = open(file1, 'a+')
            print(file1)
        f.write("\n")
        f.write(text)
        f.close()
    except:
        pass
    return ""


def tm():
    nw = datetime.now()
    thistm = nw.strftime("%Y%m%d_%H%M%S")
    return thistm

def write_into_text(contents, operation="write", filename = 'excmd', fpath = None):
    f = ''
    if filename is None:
        filename = "X"
    if fpath == None:
        flpath = os.getcwd() + filename + '_' + tm() + '.txt'
    else:
        flpath = fpath + filename + '_' + tm() + '.txt'
    content = "executed commands"
    if isinstance(contents, list):
        for i in range(len(contents)):
            content = content + chr(10) + contents[i]
    else:
        content = contents
    
    try:
        if operation == "write":
            f = open(flpath, 'w+')
        else:
            f = open(flpath, 'a+')
        f.write(content)
        f.close()
        print('print from wrt2txt, *success*', flpath, chr(10))
        return flpath
    except:
        lastslash = flpath.rfind('\\')
        flname = flpath[-lastslash :len(flpath)-4]
        print(flname)
        os.system("taskkill /F /FI '"+ flname + "' /T")
        try:
            if operation == "write":
                f = open(flpath, 'w+')
            else:
                f = open(flpath, 'a+')
            f.write(content)
            f.close()
            print('print from wrt2txt, *success*', flpath, chr(10))
            return flpath
        except:
            print('def wrt2txt *failed* ', flpath, chr(10))
            return "failed"


    