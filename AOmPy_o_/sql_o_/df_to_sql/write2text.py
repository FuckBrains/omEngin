import time as tm
from datetime import *
import os

def tm():
    nw = datetime.now()
    thistm = nw.strftime("%Y%m%d_%H%M%S")
    return thistm

def wrt2txt(contents, filename = 'excmd', flpath = None):
    if flpath == None:
        flpath = os.getcwd() + filename + '_' + tm() + '.txt'
    content = "executed commands"
    if isinstance(contents, list):
        for i in range(len(contents)):
            content = content + chr(10) + contents[i]
    else:
        content = contents
    try:
        f = open(flpath, 'w+')
        f.write(content)
        f.close()
        print('print from wrt2txt, *success*', flpath, chr(10))
    except:
        lastslash = flpath.rfind('\\')
        flname = flpath[-lastslash :len(flpath)-4]
        print(flname)
        os.system("taskkill /F /FI '"+ flname + "' /T")
        tm.sleep(2)
        try:
            f = open(flpath, 'w+')
            f.write(content)
            f.close()
            print('print from wrt2txt, *success*', flpath, chr(10))
        except:
            print('def wrt2txt *failed* ', flpath, chr(10))