
import itertools
import operator
import itertools as it
from itertools import *

def pntx(op):
    q = 0
    try:
        print(type(op), "--", op, chr(10))
    except:
        pass
    if op is not None:
        q = q + 1
        try:
            for i in op:
                print(type(i), " output: ", i)
        except:
            try:
                print("loop not worked but direct -" , op)
                try:
                    for i in range(len(op)):
                        print('loop with range worked: ' , op[i])
                except:
                    print("anther loop attempt - ")
            except:
                print("failed for: ", q)
    else:
        print('input stored None -', type(op))

class p1:
    def __init__(self, *argv):
        if argv is not None:
            for i in range(len(argv)):
                print(type(argv[i]))
            self.arg = argv
        self.opt = ''
        print("-----------------------------------")
    def printx(self):
        q = 0
        if self.opt is not None:
            q = q + 1
            try:
                for i in self.opt:
                    print(type(i), " output: ", i)
            except:
                print("failed for: ", q)
    def f2(self, ls = None):
        if ls is None:
            self.opt = itertools.accumulate(self.arg[0], operator.mul)
        else:
            self.opt = itertools.accumulate(ls4, operator.mul)
        self.printx()
    def f3(self, ls = None):
        if ls is None:
            self.opt = map(lambda x: "$" + str(x) + "$", self.arg[0])
        else:
            self.opt = map(lambda x: "$" + str(x) + "$", ls)
        self.printx()
    def f4(self, ls = None):
        self.opt = itertools.islice(ls, len(ls))
        self.printx()

ls1 = ['CXTKN80','CXTKN80','CPMTB03','NGSNG49','RPPBT09','CMDBD56','NGRPG18','NGSNG49','CPMTB21','MDSDR02']
ls2 = ['6404','6406','6542','6691','6763','7088','7105','6406','8386','7268']
ls3 = ['5/12/2020  11:54:00 AM','5/12/2020  1:07:00 PM','2020/12/5  1:43:00 PM','5/12/2020  1:46:00 PM',
        '6763','7088','7105','RPPBT09','CMDBD56','NGRPG18']
ls4 = [4,7,9]
dc1 = dict(zip(ls1,ls2))
dc2 = dict(zip(ls2,ls3))

#x = p1(ls1,ls2)
#x.f3()

OM = lambda x : map(lambda y : "'" + y + "'", ls)

def mapper(ls1,ls2):
    ls = list(map(lambda x, y: x + "='" + str(y) + "'", ls1, ls2))
    return ls

def xmapper(ls1,ls2):
    ls = list(map(lambda x, y: x + "='" + str(y) + "'", ls1, ls2))
    return ls

def f3(ls):
    op = map(lambda x: "$" + str(x) + "$", ls)



TS = lambda x : itertools.islice(x, len(x))

def parsecode(txt):
    df = pd.read_csv(os.getcwd() + '\\OMDB.csv')
    ls = df['Code'].to_list()
    for i in range(len(ls)):
        if ls[i] in txt:
            txt.find(ls[i])

