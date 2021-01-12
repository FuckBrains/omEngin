import os
import omt as om
import textfl as tx
from datetime import *

nw = datetime.now()
filename = nw.strftime("%d%m%Y-%h%m")

rpa_mask = os.getcwd() + "//rpa_group//grpmask.txt"
rpa_mask_head = os.getcwd() + "//rpa_group//grmask_head.txt"
omgrp = os.getcwd() + "//omgroup//omgrp.txt"
mask_grp = os.getcwd() + "//mask_group_map//"+ filename + "_rpa_mask_grp.txt"
rpa_mask_grp_name = os.getcwd() + "//mask_group_map//21102020-Oct10_rpa_mask_grp.txt"
rpagrpmap = os.getcwd() + "//rpa_group_map//maksmap.txt"
rpagrpnm = os.getcwd() + "//rpa_group_map//grpname.txt"

def update_files_rpa():
    tx.pick_rpa_group()

def update_file_omgrp():
    om.client_run()

def map_text_by_mask(f1,f2):
    st = ""
    file1 = tx.rdall(f1)
    file2 = tx.rdline(f2)
    for line in file2:
        if len(line)>5:
            msk = line[0:line.find(",")-1]
            if msk in file1:
                st = st + "\n" + line
    tx.wrt(mask_grp,st)
    
def map_by_groupname(f1):
    f2 = om.client_run()
    st = ""
    m = ""
    file1 = tx.rdline(f1)
    file2 = tx.rdline(f2)
    for line in file2:
        if len(line)>5:
            msk = line[0:line.find(",")-1]
            grpname = line[line.find(",")+1:len(line)-1]
            for ln in file1:
                gn = ln[ln.find(",")+1:len(ln)-1]
                old_msk = ln[0:ln.find(",")-1]
                print(gn)
                if gn == grpname:
                    print(gn,grpname)
                               

def src_in_file(content,srcstr):
    for ln in content:
        lnx = ln.replace("\n","")
        comma = lnx.find(',')
        msk = lnx[0:comma]
        gnm = lnx[comma+1:len(lnx)]
        if gnm == srcstr:
            return msk
            break

def old_new_mask_map():
    oldref = tx.rdline(rpa_mask_grp_name)
    f2 = om.client_run()
    fl2 = tx.rdline(f2)
    n = 0
    st = ""
    currgrp = ""
    for ln in oldref:
        if len(ln)>5:
            n = n + 1
            lnx = ln.replace("\n","")
            comma = lnx.find(',')
            msk = lnx[0:comma]
            gnm = lnx[comma+1:len(lnx)]
            ms = src_in_file(fl2,gnm)
            if type(ms) != 'NoneType':
                if st == "":
                    st = str(msk) + "," + str(ms)
                    currgrp = str(n) + ". " + gnm
                else:
                    st = st + chr(10) + str(msk) + "," + str(ms)
                    currgrp = currgrp + chr(10) + str(n) + ". " + gnm
    tx.wrt(rpagrpmap,st)
    tx.wrt(rpagrpnm,currgrp)
    
old_new_mask_map()







