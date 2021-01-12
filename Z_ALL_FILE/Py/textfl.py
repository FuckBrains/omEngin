import os

fpth = os.getcwd() + "//txtfile"
grp = os.getcwd() + "//rpa_group"
grponlymask = grp + "//grpmask.txt"
grpmask_and_head = grp + "//grmask_head.txt"

def srctxt_exact(content,srcstr):
    ls = []
    for ln in content:
        lnx = ln.strip()
        if srcstr == lnx:
            ls.append(lnx)
    return ls
            
def srctxt_partial(content,srcstr):
    ls = []
    for ln in content:
        lnx = ln.strip()
        if srcstr in lnx:
            ls.append(lnx)
    return ls

def rdline(flname):
    fl  = open(flname, "r") 
    contents =fl.readlines()
    fl.close()
    return contents
        
def rdall(flname):
    fl  = open(flname, "r") 
    contents =fl.read()
    fl.close()
    return contents
    
def wrt(flname,content):
    fl  = open(flname, "w+", encoding="utf-8")
    fl.write(content)
    fl.close()
    
def apnd(flname,content):
    fl  = open(flname, "a+")
    fl.write(content)
    fl.close()
    
def get_list_txt_file(dirr):
    fl = os.listdir(dirr)
    ls = []
    for f in fl:
        if f.endswith('.txt'):
            ls.append(f)
    return ls

def pick_rpa_group():
    lst = get_list_txt_file(fpth)
    st = ""
    sthd = ""
    for i in range(len(lst)):
        pth = fpth + "//" + lst[i]
        contents = rdline(pth)
        n = 0
        for f in contents:
            fr = f.find('-R')
            fd = f.find('$')
            grpname = f[0:fr-1]
            grpmask = f[fd+1:len(f)-1]
            grpcon = grpmask + "," + grpname
            if grpmask not in st:
                st = st + "\n" + grpmask
            if grpmask not in sthd:
                n = n + 1
                sthd = sthd + "\n" + grpcon
        print(st)
        print(sthd)
        wrt(grponlymask,st)
        wrt(grpmask_and_head,sthd)
