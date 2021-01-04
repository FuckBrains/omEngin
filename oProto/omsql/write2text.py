import os
from datetime import *

def tm():
    nw = datetime.now()
    thistm = nw.strftime("%Y%m%d_%H%M%S")
    return thistm

def wrt2txt(contents, filename = 'excmd', fpath = None):
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
        f = open(flpath, 'w+')
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
            f = open(flpath, 'w+')
            f.write(content)
            f.close()
            print('print from wrt2txt, *success*', flpath, chr(10))
            return flpath
        except:
            print('def wrt2txt *failed* ', flpath, chr(10))
            return "failed"
    